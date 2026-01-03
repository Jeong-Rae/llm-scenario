const { EventEmitter } = require("events");
const { ConversationModel } = require("../model/llm");
const { FakeAI } = require("./fake-ai");

// 기술 블로그 예시: 인메모리 토큰 버퍼(읽기 시점 기준).
const tokenBuffer = new Map(); // 키: `${conversationId}:${messageId}` -> string[]
const tokenStatus = new Map(); // 키: `${conversationId}:${messageId}` -> { done: boolean }
// 기술 블로그 예시: 동일 키 중복 요청 감지용 레지스트리.
const requestKeys = new Set();
// 기술 블로그 예시: 마지막 읽기 시점(읽기 이후에만 토큰을 보관).
const lastReadAt = new Map(); // 키: `${conversationId}:${messageId}` -> number

const STREAM_CHUNK_SIZE = 12;
const STREAM_DELAY_MS = 30;
const STREAM_JITTER_MS = 40;
const ACTIVE_WINDOW_MS = 800;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomInt = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const chunkText = (text, size) => {
  const chunks = [];
  for (let i = 0; i < text.length; i += size) {
    chunks.push(text.slice(i, i + size));
  }
  return chunks;
};

const createLangChainClient = () =>
  new FakeAI({
    chunkSize: STREAM_CHUNK_SIZE,
    delayMs: STREAM_DELAY_MS,
    jitterMs: STREAM_JITTER_MS,
  });

const normalizeChunk = (chunk) => {
  if (typeof chunk.content === "string") return chunk.content;
  if (Array.isArray(chunk.content)) {
    return chunk.content.map((part) => part.text ?? "").join("");
  }
  return "";
};

class LLMService {
  constructor() {
    this.client = createLangChainClient();
  }

  // 1) POST -> 토큰 스트림을 클라이언트로 보내며 fullOutput 누적 후 저장.
  async streamAndPersist({ conversationId, messageId, prompt }) {
    const requestKey = `${conversationId}:${messageId}`;
    if (requestKeys.has(requestKey)) {
      console.log(`[중복] 스트림 요청 키가 이미 사용 중: ${requestKey}`);
    }
    requestKeys.add(requestKey);
    tokenStatus.set(requestKey, { done: false });
    const emitter = new EventEmitter();
    let fullOutput = "";

    (async () => {
      try {
        const stream = await this.client.stream(prompt);
        for await (const chunk of stream) {
          const text = normalizeChunk(chunk);
          fullOutput += text;
          emitter.emit("token", text);
        }
        await ConversationModel.updateOne(
          { _id: conversationId, "messages.messageId": messageId },
          {
            $setOnInsert: { _id: conversationId, messages: [] },
            $push: { messages: { messageId, content: fullOutput } },
          },
          { upsert: true }
        );
        emitter.emit("done", { conversationId, messageId });
      } catch (error) {
        emitter.emit("error", error);
      } finally {
        requestKeys.delete(requestKey);
        tokenStatus.set(requestKey, { done: true });
      }
    })();

    return emitter;
  }

  // 2) POST -> invoke 후 토큰을 메모리에 저장, GET으로 조회.
  async invokeAndCache({ conversationId, messageId, prompt }) {
    const cacheKey = `${conversationId}:${messageId}`;
    if (requestKeys.has(cacheKey)) {
      console.log(`[중복] invoke 요청 키가 이미 사용 중: ${cacheKey}`);
    }
    requestKeys.add(cacheKey);
    if (tokenBuffer.has(cacheKey)) {
      console.log(`[중복] 토큰 캐시 키가 이미 존재함: ${cacheKey}`);
    }
    tokenBuffer.set(cacheKey, []);
    lastReadAt.set(cacheKey, Date.now());
    tokenStatus.set(cacheKey, { done: false });

    (async () => {
      try {
        let fullOutput = "";
        const response = await this.client.invoke([{ content: prompt }]);
        const chunks = chunkText(response.content, STREAM_CHUNK_SIZE);
        for (const chunk of chunks) {
          const jitter = STREAM_JITTER_MS > 0 ? randomInt(0, STREAM_JITTER_MS) : 0;
          await sleep(STREAM_DELAY_MS + jitter);
          const lastRead = lastReadAt.get(cacheKey) ?? 0;
          if (Date.now() - lastRead <= ACTIVE_WINDOW_MS) {
            tokenBuffer.get(cacheKey).push(chunk);
          }
          fullOutput += chunk;
        }
        await ConversationModel.updateOne(
          { _id: conversationId, "messages.messageId": messageId },
          {
            $setOnInsert: { _id: conversationId, messages: [] },
            $push: { messages: { messageId, content: fullOutput } },
          },
          { upsert: true }
        );
      } catch (error) {
        tokenBuffer.set(cacheKey, [`오류: ${error.message ?? "요청 실패"} `]);
      } finally {
        requestKeys.delete(cacheKey);
        tokenStatus.set(cacheKey, { done: true });
      }
    })();

    return { conversationId, messageId };
  }

  getCachedTokens({ conversationId, messageId }) {
    const cacheKey = `${conversationId}:${messageId}`;
    lastReadAt.set(cacheKey, Date.now());
    const status = tokenStatus.get(cacheKey) ?? { done: false };
    const tokens = tokenBuffer.get(cacheKey) ?? [];
    tokenBuffer.set(cacheKey, []);
    return { tokens, done: status.done };
  }

  // 3) 커서 기준으로 DB 토큰을 리플레이한 뒤 캐시 tail을 이어 붙임.
  async getTokensWithCursor({ conversationId, messageId, cursor = 0 }) {
    const doc = await ConversationModel.findOne({ _id: conversationId });
    const message = doc?.messages.find((m) => m.messageId === messageId);
    const fullOutput = message?.content ?? "";
    const persistedTokens = fullOutput.split(" ").map((t) => `${t} `);
    const backlog = persistedTokens.slice(cursor);

    const cacheKey = `${conversationId}:${messageId}`;
    const tail = tokenCache.get(cacheKey) ?? [];

    return { tokens: [...backlog, ...tail], nextCursor: persistedTokens.length };
  }

  // 4) 리플레이와 실시간 캐시 사이의 경합 누락을 버퍼로 보정.
  async getTokensWithCursorAndBuffer({ conversationId, messageId, cursor = 0 }) {
    const cacheKey = `${conversationId}:${messageId}`;
    const buffer = [];
    let live = false;

    const readTail = () => tokenCache.get(cacheKey) ?? [];
    const tailSnapshot = readTail();
    buffer.push(...tailSnapshot);

    const doc = await ConversationModel.findOne({ _id: conversationId });
    const message = doc?.messages.find((m) => m.messageId === messageId);
    const fullOutput = message?.content ?? "";
    const persistedTokens = fullOutput.split(" ").map((t) => `${t} `);
    const backlog = persistedTokens.slice(cursor);

    const cutoff = tailSnapshot.length;
    const safeTail = buffer.slice(cutoff);
    live = true;

    return {
      tokens: [...backlog, ...safeTail],
      nextCursor: persistedTokens.length,
      live,
    };
  }
}

module.exports = {
  LLMService,
};
