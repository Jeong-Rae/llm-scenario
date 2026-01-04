import { Application, Request, Response } from "express";
import { LLMService } from "../service/llm";

const service = new LLMService();

/**
 * 1. 간단한 One Pahse LLM Chat 구조
 * Post 요청으로 keepalive chunk 형식으로 SSE와 같은 요청을 보낸다.
 * body로 conversationId를 보내고, server에서 conversation의 가장 마지막 messageId를 만들어준다.
 * client에 stream으로 conversationId와 messageId를 먼저 응답을 보내준다.
 * 이후 AI에게 사용자의 message를 보내고, ai에서 stream이 도착하면, 다시 client stream으로 emit한다.
 * ai 응답이 종료되면, 결과는 FakeMongoModel에 저장하고, client에게는 done 응답을 보내며 sse 연결을 close한다.
 * 
 * // 확인하려는 목표
 * // Post 응답이 도중에 Web이 끊기면(새로고침이나 이전페이지로 가기 등) 사용자는 이후 메시지를 볼수 없음. 
 * // GET /conversation/:id를 통해서만이 확인할수있는, 즉 생성중에는 오히려 스트리밍되던 데이터가 없어서 과거로 롤백되는것처럼보이는 문제가 있음을 Web에서 보여야함.
 */

/**
 * 2. Write와 Read를 분리한 Two Phase LLM Chat 구조
 * Post요청으로 conversationId와 Message를 보낸다.
 * 즉시 AI에게 stream 요청을 보내고, client에는 MessageId를 response로 보낸다.
 * 이후 Map에 conversationId, messageId, chunks: [], done,  error, emitter를 저장한다. Key는 conversationId+messageId이다.
 * 
 * 이후 Client는 GET을 통해 SSE를 보내고, 거기에는 conversationId와 messageId를 쿼리로 포함시켜서 보낸다.
 * 
 * Post 세션에서 AI응답을 stream으로 받으면 Map에 chunk를 추가하고, emitter를 통해 이벤트를 방출한다.
 * 
 * Get 세션에서는 conversationId와 messageId를 쿼리로보내고, 그를 통해 emmiter를 가져온다.
 * 이후 emmiter가 보내는 chunk 발행 및 done 이벤트를 sub한다.
 * sub한 데이터를 sse stream을 통해 client에게 스트리밍해준다. done되면 FakeMongoModel에 저장하고 sse연결을 close한다.
 * 
 * 
 * 
 * // 확인하려는 목표
 * // GET sse가 도중에 끊겨도, 다시 재연결하면 동일한 message에 대해 즉시 stream을 다시 받을수 있음을 통해 stream 읽기 안정성 확보
 * // 첫번째 SSE와 두번째 SSE 사이 재연결 시간 사이에 chunk는 받지모하는 문제가 있음을 UI에서 보여야함.
 * 
 */

/**
 * 3. Cursor와 Replay가 추가된 Chat Stream 구조
 * (2.)와 동일한 구조에 SSE 동작에 Cursor(chunk seq)를 추가한다. seq기준으로 가장 최근까지의 chunks를 전부 전달한다.
 * client에게 해당 chunks를 하나의 chunk로 전달하고, 이후 emit 받는 데이터를 client에게 stream 보낸다.
 * 
 * // 확인하려는 목표
 * // SSE가 중간합류하더라도 연결되기 이전까지의 데이터를 놓치는 것 없이 받을 수 있음을 보일수있다.
 * // 이전까지 replay데이터와, emit 리스닝 사이에 이벤트가 발행되는경우, 그 사이에 이벤트를 누락할 수 있는 문제를 보여야한다.
 */

/**
 * 4. Buffer가 추가된 Chat Stream 구조
 * (3.)와 동일한 구조에 Buffer를 추가한다.
 * 이벤트 emit을 먼저받고, buffer에 쌓는다. 이후 chuks를 전부 보내고, buffer를 우선적으로 처리하고,
 *  buffer가 비어야 live chunk를 client에게 스트리밍한다.
 * 
 * // 확인하려는 목표
 * // replay데이터와 emit 리스닝 사이에 발생되는 이벤트도 놓치지않고 전부 받을수있다.
 */

type ChatBody = {
  conversationId?: string;
  message?: string;
};

type ReadQuery = {
  conversationId?: string;
  messageId?: string;
  cursor?: string;
};

type ConversationParams = {
  id: string;
};

const setSseHeaders = (res: Response) => {
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();
};

const serializeOffset = (messageId: string, seq: number) =>
  `${messageId}:${seq}`;

const parseOffset = (value?: string) => {
  if (!value) return null;
  const separatorIndex = value.lastIndexOf(":");
  if (separatorIndex <= 0) return null;
  const messageId = value.slice(0, separatorIndex);
  const seq = Number.parseInt(value.slice(separatorIndex + 1), 10);
  if (Number.isNaN(seq)) return null;
  return { messageId, seq };
};

const writeSse = (
  res: Response,
  event: string,
  data?: unknown,
  id?: string
) => {
  if (res.writableEnded) return;
  res.write(`event: ${event}\n`);
  if (id) res.write(`id: ${id}\n`);
  if (data === undefined) {
    res.write("data: \n\n");
    console.log(`SSE sent event: ${event}, id: ${id ?? "-"}`);
    return;
  }

  if (typeof data === "string") {
    const lines = data.split(/\r?\n/);
    for (const line of lines) {
      res.write(`data: ${line}\n`);
    }
    res.write("\n");
    console.log(
      `SSE sent event: ${event}, id: ${id ?? "-"}, data: ${data}`
    );
    return;
  }

  const payload = JSON.stringify(data);
  res.write(`data: ${payload}\n\n`);
  console.log(`SSE sent event: ${event}, id: ${id ?? "-"}, data: ${payload}`);
};

const startHeartbeat = (res: Response) => {
  const interval = setInterval(() => {
    if (!res.writableEnded) res.write(":keep-alive\n\n");
  }, 15000);
  return () => clearInterval(interval);
};

const parseCursor = (value?: string) => {
  if (!value) return -1;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? -1 : parsed;
};

const getCursor = (
  req: Request<Record<string, never>, unknown, unknown, ReadQuery>,
  messageId?: string
) => {
  const header = req.headers["last-event-id"];
  if (typeof header === "string") {
    const parsed = parseOffset(header);
    if (parsed && (!messageId || parsed.messageId === messageId)) {
      return parsed.seq;
    }
  }
  return parseCursor(req.query.cursor);
};

const postStream = async (
  req: Request<Record<string, never>, unknown, ChatBody>,
  res: Response
) => {
  const { conversationId: inputConversationId, message } = req.body ?? {};
  const conversationId =
    inputConversationId || service.createConversationId();
  const messageId = service.createMessageId(conversationId);

  setSseHeaders(res);
  const stopHeartbeat = startHeartbeat(res);
  let closed = false;
  const handleClose = () => {
    closed = true;
    stopHeartbeat();
  };

  req.on("aborted", handleClose);
  res.on("close", handleClose);

  writeSse(res, "start", { conversationId, messageId });
  let seq = 0;

  await service.streamOnePhase({
    conversationId,
    messageId,
    userMessage: message,
    onChunk: ({ content }) => {
      if (closed) return;
      const chunkId = serializeOffset(messageId, seq);
      seq += 1;
      writeSse(res, "chunk", content, chunkId);
    },
    onDone: () => {
      if (closed) return;
      const doneId = seq > 0 ? serializeOffset(messageId, seq - 1) : undefined;
      writeSse(res, "done", { messageId }, doneId);
      stopHeartbeat();
      res.end();
    },
    onError: (error) => {
      if (closed) return;
      writeSse(res, "error", { message: error.message });
      stopHeartbeat();
      res.end();
    },
  });
};

const postStartWrite = (
  req: Request<Record<string, never>, unknown, ChatBody>,
  res: Response
) => {
  const { conversationId: inputConversationId, message } = req.body ?? {};
  const conversationId =
    inputConversationId || service.createConversationId();
  const messageId = service.createMessageId(conversationId);

  service.startWrite({ conversationId, messageId, userMessage: message });
  res.status(202).json({ conversationId, messageId });
};

const getReadWindow = (
  req: Request<Record<string, never>, unknown, unknown, ReadQuery>,
  res: Response
) => {
  const { conversationId, messageId } = req.query;
  const session = service.getSession(conversationId, messageId);

  if (!session) {
    res.status(404).json({ error: "unknown conversation/message" });
    return;
  }

  setSseHeaders(res);
  const stopHeartbeat = startHeartbeat(res);

  const lastSeq = session.nextSeq - 1;
  const lastSeqId =
    lastSeq >= 0 ? serializeOffset(session.messageId, lastSeq) : undefined;

  if (session.error) {
    writeSse(res, "error", { message: session.error.message }, lastSeqId);
    stopHeartbeat();
    res.end();
    return;
  }

  if (session.done) {
    writeSse(res, "done", { messageId }, lastSeqId);
    stopHeartbeat();
    res.end();
    return;
  }

  const onChunk = (chunk: { seq: number; content: string }) =>
    writeSse(
      res,
      "chunk",
      chunk.content,
      serializeOffset(session.messageId, chunk.seq)
    );
  const onDone = (payload: { messageId: string }) => {
    const doneSeq = session.nextSeq - 1;
    const doneId =
      doneSeq >= 0 ? serializeOffset(session.messageId, doneSeq) : undefined;
    writeSse(res, "done", payload, doneId);
    cleanup();
  };
  const onError = (error: Error) => {
    writeSse(res, "error", { message: error.message });
    cleanup();
  };
  const cleanup = () => {
    session.emitter.off("chunk", onChunk);
    session.emitter.off("done", onDone);
    session.emitter.off("error", onError);
    stopHeartbeat();
    if (!res.writableEnded) res.end();
  };

  session.emitter.on("chunk", onChunk);
  session.emitter.on("done", onDone);
  session.emitter.on("error", onError);

  req.on("close", cleanup);
};

const getReplayWithCursor = (
  req: Request<Record<string, never>, unknown, unknown, ReadQuery>,
  res: Response
) => {
  const { conversationId, messageId } = req.query;
  const session = service.getSession(conversationId, messageId);

  if (!session) {
    res.status(404).json({ error: "unknown conversation/message" });
    return;
  }

  setSseHeaders(res);
  const stopHeartbeat = startHeartbeat(res);

  const cursor = getCursor(req, session.messageId);
  const replayChunks = session.chunks.filter((chunk) => chunk.seq > cursor);

  if (replayChunks.length > 0) {
    const replayContent = replayChunks.map((chunk) => chunk.content).join("");
    const replayId = serializeOffset(
      session.messageId,
      replayChunks[replayChunks.length - 1].seq
    );
    writeSse(res, "replay", replayContent, replayId);
  }

  if (session.error) {
    const lastSeq = session.nextSeq - 1;
    const lastSeqId =
      lastSeq >= 0 ? serializeOffset(session.messageId, lastSeq) : undefined;
    writeSse(res, "error", { message: session.error.message }, lastSeqId);
    stopHeartbeat();
    res.end();
    return;
  }

  if (session.done) {
    const lastSeq = session.nextSeq - 1;
    const lastSeqId =
      lastSeq >= 0 ? serializeOffset(session.messageId, lastSeq) : undefined;
    writeSse(res, "done", { messageId }, lastSeqId);
    stopHeartbeat();
    res.end();
    return;
  }

  const onChunk = (chunk: { seq: number; content: string }) =>
    writeSse(
      res,
      "chunk",
      chunk.content,
      serializeOffset(session.messageId, chunk.seq)
    );
  const onDone = (payload: { messageId: string }) => {
    const doneSeq = session.nextSeq - 1;
    const doneId =
      doneSeq >= 0 ? serializeOffset(session.messageId, doneSeq) : undefined;
    writeSse(res, "done", payload, doneId);
    cleanup();
  };
  const onError = (error: Error) => {
    writeSse(res, "error", { message: error.message });
    cleanup();
  };
  const cleanup = () => {
    session.emitter.off("chunk", onChunk);
    session.emitter.off("done", onDone);
    session.emitter.off("error", onError);
    stopHeartbeat();
    if (!res.writableEnded) res.end();
  };

  session.emitter.on("chunk", onChunk);
  session.emitter.on("done", onDone);
  session.emitter.on("error", onError);

  req.on("close", cleanup);
};

const getReplayWithBuffer = (
  req: Request<Record<string, never>, unknown, unknown, ReadQuery>,
  res: Response
) => {
  const { conversationId, messageId } = req.query;
  const session = service.getSession(conversationId, messageId);

  if (!session) {
    res.status(404).json({ error: "unknown conversation/message" });
    return;
  }

  setSseHeaders(res);
  const stopHeartbeat = startHeartbeat(res);

  const buffer: Array<{ seq: number; content: string }> = [];
  let live = false;
  let donePayload: { messageId: string } | null = null;
  let errorPayload: Error | null = null;

  const onChunk = (chunk: { seq: number; content: string }) => {
    if (!live) {
      buffer.push(chunk);
      return;
    }
    writeSse(
      res,
      "chunk",
      chunk.content,
      serializeOffset(session.messageId, chunk.seq)
    );
  };
  const onDone = (payload: { messageId: string }) => {
    if (!live) {
      donePayload = payload;
      return;
    }
    const doneSeq = session.nextSeq - 1;
    const doneId =
      doneSeq >= 0 ? serializeOffset(session.messageId, doneSeq) : undefined;
    writeSse(res, "done", payload, doneId);
    cleanup();
  };
  const onError = (error: Error) => {
    if (!live) {
      errorPayload = error;
      return;
    }
    writeSse(res, "error", { message: error.message });
    cleanup();
  };
  const cleanup = () => {
    session.emitter.off("chunk", onChunk);
    session.emitter.off("done", onDone);
    session.emitter.off("error", onError);
    stopHeartbeat();
    if (!res.writableEnded) res.end();
  };

  session.emitter.on("chunk", onChunk);
  session.emitter.on("done", onDone);
  session.emitter.on("error", onError);

  const cursor = getCursor(req, session.messageId);
  const replayChunks = session.chunks.filter((chunk) => chunk.seq > cursor);

  if (replayChunks.length > 0) {
    const replayContent = replayChunks.map((chunk) => chunk.content).join("");
    const replayId = serializeOffset(
      session.messageId,
      replayChunks[replayChunks.length - 1].seq
    );
    writeSse(res, "replay", replayContent, replayId);
  }

  const drainBuffer = () => {
    if (buffer.length === 0) return;
    buffer.sort((a, b) => a.seq - b.seq);
    for (const chunk of buffer.splice(0)) {
      writeSse(
        res,
        "chunk",
        chunk.content,
        serializeOffset(session.messageId, chunk.seq)
      );
    }
  };

  drainBuffer();
  live = true;

  if (donePayload || session.done) {
    const doneSeq = session.nextSeq - 1;
    const doneId =
      doneSeq >= 0 ? serializeOffset(session.messageId, doneSeq) : undefined;
    writeSse(res, "done", donePayload ?? { messageId }, doneId);
    cleanup();
    return;
  }

  req.on("close", cleanup);
};

const registerRoutes = (app: Application) => {
  app.post("/chat/one-phase", postStream);
  app.post("/chat/write-start", postStartWrite);
  app.get("/chat/read-window", getReadWindow);
  app.get("/chat/replay", getReplayWithCursor);
  app.get("/chat/replay-buffer", getReplayWithBuffer);
  app.get(
    "/conversation/:id",
    async (req: Request<ConversationParams>, res: Response) => {
    const conversation = await service.getConversation(req.params.id);
    if (!conversation) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.json(conversation);
  });
};

export {
  registerRoutes,
  postStream,
  postStartWrite,
  getReadWindow,
  getReplayWithCursor,
  getReplayWithBuffer,
};
