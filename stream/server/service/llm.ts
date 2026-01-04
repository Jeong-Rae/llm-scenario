import { EventEmitter } from "events";
import { randomUUID } from "crypto";
import { FakeAI } from "./fake-ai";
import { ConversationModel } from "../model/llm";

type AIChunk = { content: string };
type AIStream = { stream: () => AsyncGenerator<AIChunk, void, void> };
type AIFactory = (input: { userMessage?: string }) => AIStream;

type StreamChunk = { seq: number; content: string };
type StreamSession = {
  conversationId: string;
  messageId: string;
  chunks: StreamChunk[];
  done: boolean;
  error: Error | null;
  emitter: EventEmitter;
  nextSeq: number;
};

type StreamOnePhaseOptions = {
  conversationId: string;
  messageId: string;
  userMessage?: string;
  onChunk?: (chunk: AIChunk) => void;
  onDone?: (payload: { content: string }) => void;
  onError?: (error: Error) => void;
};

type StartWriteOptions = {
  conversationId: string;
  messageId: string;
  userMessage?: string;
};


class LLMService {
  private aiFactory: AIFactory;
  private sessions: Map<string, StreamSession>;
  private lastMessageIdByConversation: Map<string, string>;

  constructor({ aiFactory }: { aiFactory?: AIFactory } = {}) {
    this.aiFactory = aiFactory ?? ((_input) => new FakeAI());
    this.sessions = new Map();
    this.lastMessageIdByConversation = new Map();
  }

  createConversationId(): string {
    return randomUUID();
  }

  createMessageId(conversationId?: string): string {
    const messageId = randomUUID();
    if (conversationId) {
      const previousId = this.lastMessageIdByConversation.get(conversationId);
      if (previousId) {
        this.sessions.delete(this._key(conversationId, previousId));
      }
      this.lastMessageIdByConversation.set(conversationId, messageId);
    }
    return messageId;
  }

  getSession(conversationId?: string, messageId?: string): StreamSession | null {
    if (!conversationId || !messageId) return null;
    return this.sessions.get(this._key(conversationId, messageId)) ?? null;
  }

  ensureSession(conversationId: string, messageId: string): StreamSession {
    const key = this._key(conversationId, messageId);
    const existing = this.sessions.get(key);
    if (existing) return existing;

    const session: StreamSession = {
      conversationId,
      messageId,
      chunks: [],
      done: false,
      error: null,
      emitter: new EventEmitter(),
      nextSeq: 0,
    };
    session.emitter.setMaxListeners(50);
    this.sessions.set(key, session);
    return session;
  }

  async streamOnePhase({
    conversationId,
    messageId,
    userMessage,
    onChunk,
    onDone,
    onError,
  }: StreamOnePhaseOptions): Promise<void> {
    try {
      const ai = this.aiFactory({ userMessage });
      const chunks: string[] = [];
      for await (const { content } of ai.stream()) {
        chunks.push(content);
        onChunk?.({ content });
      }
      const content = chunks.join("");
      await this._saveMessage(conversationId, messageId, content);
      onDone?.({ content });
    } catch (error) {
      const normalized =
        error instanceof Error ? error : new Error("unknown error");
      onError?.(normalized);
    }
  }

  startWrite({ conversationId, messageId, userMessage }: StartWriteOptions) {
    const session = this.ensureSession(conversationId, messageId);
    this._streamToSession(session, userMessage);
    return session;
  }

  async getConversation(conversationId: string) {
    return ConversationModel.findOne({ _id: conversationId });
  }

  private _key(conversationId: string, messageId: string): string {
    return `${conversationId}:${messageId}`;
  }

  private async _streamToSession(
    session: StreamSession,
    userMessage?: string
  ): Promise<void> {
    try {
      const ai = this.aiFactory({ userMessage });
      for await (const { content } of ai.stream()) {
        const chunk = { seq: session.nextSeq, content };
        session.nextSeq += 1;
        session.chunks.push(chunk);
        session.emitter.emit("chunk", chunk);
      }
      session.done = true;
      const content = session.chunks.map((chunk) => chunk.content).join("");
      await this._saveMessage(session.conversationId, session.messageId, content);
      session.emitter.emit("done", { messageId: session.messageId });
    } catch (error) {
      const normalized =
        error instanceof Error ? error : new Error("unknown error");
      session.error = normalized;
      session.emitter.emit("error", normalized);
    }
  }

  private async _saveMessage(
    conversationId: string,
    messageId: string,
    content: string
  ): Promise<void> {
    await ConversationModel.updateOne(
      { _id: conversationId },
      {
        $setOnInsert: { _id: conversationId },
        $push: {
          messages: {
            messageId,
            content,
            role: "agent",
            timestamp: new Date().toISOString(),
          },
        },
      },
      { upsert: true }
    );
  }
}

export { LLMService };
