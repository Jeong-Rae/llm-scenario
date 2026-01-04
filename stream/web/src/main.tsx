import { useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { Button, Textarea } from "@vapor-ui/core";
import "@vapor-ui/core/styles.css";
import "./styles.css";

const apiFetch = (path: string, options?: RequestInit) =>
  fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

type SseEvent = {
  event: string;
  data: string;
  id: string;
};

type SsePayload = {
  data: string;
  id: string;
};

type SseErrorPayload = {
  data: string | null;
  id: string;
};

type EventSourceHandlers = {
  onChunk?: (payload: SsePayload) => void;
  onReplay?: (payload: SsePayload) => void;
  onDone?: (payload: SsePayload) => void;
  onError?: (payload: SseErrorPayload) => void;
};

const readPostSseStream = async (
  res: Response,
  onEvent: (event: SseEvent) => void,
  signal?: AbortSignal
) => {
  if (!res.body) return;
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    if (signal?.aborted) {
      await reader.cancel();
      break;
    }
    const { value, done } = await reader.read();
    if (done) break;
    if (!value) continue;
    buffer += decoder.decode(value, { stream: true });
    const parts = buffer.split("\n\n");
    buffer = parts.pop() ?? "";

    for (const part of parts) {
      const lines = part.split("\n");
      let event = "message";
      let id = "";
      const dataLines: string[] = [];

      for (const line of lines) {
        if (!line || line.startsWith(":")) continue;
        if (line.startsWith("event:")) {
          event = line.slice("event:".length).trim();
          continue;
        }
        if (line.startsWith("id:")) {
          id = line.slice("id:".length).trim();
          continue;
        }
        if (line.startsWith("data:")) {
          dataLines.push(line.slice("data:".length).trim());
        }
      }

      if (dataLines.length === 0) continue;
      onEvent({ event, data: dataLines.join("\n"), id });
    }
  }
};

const openEventSource = (url: string, handlers: EventSourceHandlers) => {
  const source = new EventSource(url);
  let closed = false;
  const close = () => source.close();

  const bindMessage = (
    eventName: "chunk" | "replay" | "done",
    handler?: (payload: SsePayload) => void
  ) => {
    if (!handler) return;
    source.addEventListener(eventName, (event) => {
      const message = event as MessageEvent<string>;
      handler({ data: message.data ?? "", id: message.lastEventId ?? "" });
      if (eventName === "done") {
        closed = true;
        close();
      }
    });
  };

  bindMessage("chunk", handlers.onChunk);
  bindMessage("replay", handlers.onReplay);
  bindMessage("done", handlers.onDone);

  source.addEventListener("error", (event) => {
    const message = event as MessageEvent<string>;
    const data = typeof message.data === "string" ? message.data : null;
    if (closed) return;
    handlers.onError?.({ data, id: message.lastEventId ?? "" });
    close();
  });

  return source;
};

const parseSeqFromEventId = (value: string) => {
  if (!value) return null;
  const separatorIndex = value.lastIndexOf(":");
  if (separatorIndex <= 0) return null;
  const seq = Number.parseInt(value.slice(separatorIndex + 1), 10);
  return Number.isNaN(seq) ? null : seq;
};

const useConversationId = (prefix: string): string => {
  const now = useMemo(() => Date.now().toString(36).slice(-6), []);
  return `${prefix}-c-${now}`;
};

const StreamCard = () => {
  const conversationId = useConversationId("s1");
  const abortRef = useRef<AbortController | null>(null);
  const [prompt, setPrompt] = useState<string>("간단한 인사말을 작성해줘.");
  const [output, setOutput] = useState<string>("");
  const [busy, setBusy] = useState<boolean>(false);

  const startStream = async () => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    setBusy(true);
    setOutput("");
    try {
      const res = await apiFetch("/chat/one-phase", {
        method: "POST",
        body: JSON.stringify({ conversationId, message: prompt }),
        signal: controller.signal,
      });
    await readPostSseStream(
      res,
      ({ event, data }) => {
        if (event === "chunk") {
          setOutput((prev) => prev + data);
        }
        if (event === "done" || event === "error") {
          setBusy(false);
        }
      },
      controller.signal
    );
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        return;
      }
    } finally {
      setBusy(false);
    }
  };

  const stopStream = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setBusy(false);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 1 · POST + 스트리밍</span>
      <div className="row">
        <Textarea
          className="prompt-input"
          autoResize={false}
          value={prompt}
          onValueChange={(value) => setPrompt(value)}
        />
      </div>
      <div className="meta">총 응답 길이: {output.length}</div>
      <div className="row">
        <Button onClick={startStream} disabled={busy}>
          {busy ? "스트리밍 중..." : "스트림 시작"}
        </Button>
      </div>
      <div className="row">
        <Button onClick={stopStream} disabled={!busy}>
          중지
        </Button>
      </div>
      <div className="output">{output}</div>
    </div>
  );
};

const CacheCard = () => {
  const conversationId = useConversationId("s2");
  const sourceRef = useRef<EventSource | null>(null);
  const [prompt, setPrompt] = useState<string>("영화 장면을 요약해줘.");
  const [output, setOutput] = useState<string>("");
  const [busy, setBusy] = useState<boolean>(false);
  const [messageId, setMessageId] = useState<string | null>(null);
  const [listening, setListening] = useState<boolean>(false);

  const startReadWindow = async (nextConversationId: string, id: string) => {
    sourceRef.current?.close();
    const url = `/chat/read-window?conversationId=${nextConversationId}&messageId=${id}`;
    const source = openEventSource(url, {
      onChunk: ({ data }) => {
        setOutput((prev) => prev + data);
      },
      onDone: () => {
        setListening(false);
      },
      onError: () => {
        setListening(false);
      },
    });
    sourceRef.current = source;
    setListening(true);
  };

  const invoke = async () => {
    setBusy(true);
    setOutput("");
    const res = await apiFetch("/chat/write-start", {
      method: "POST",
      body: JSON.stringify({ conversationId, message: prompt }),
    });
    const data = (await res.json()) as {
      conversationId: string;
      messageId: string;
    };
    setMessageId(data.messageId);
    setBusy(false);
    await startReadWindow(data.conversationId, data.messageId);
  };

  const stopReadWindow = () => {
    sourceRef.current?.close();
    sourceRef.current = null;
    setListening(false);
  };

  const resumeReadWindow = () => {
    if (!messageId || listening) return;
    startReadWindow(conversationId, messageId);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 2 · POST(쓰기) + GET(읽기)</span>
      <div className="row">
        <Textarea
          className="prompt-input"
          autoResize={false}
          value={prompt}
          onValueChange={(value) => setPrompt(value)}
        />
      </div>
      <div className="meta">총 응답 길이: {output.length}</div>
      <div className="row">
        <Button className="secondary" onClick={invoke} disabled={busy}>
          {busy ? "요청 중..." : "요청"}
        </Button>
      </div>
      <div className="row">
        <Button onClick={stopReadWindow} disabled={!listening}>
          중지
        </Button>
        <Button
          className="secondary"
          onClick={resumeReadWindow}
          disabled={!messageId || listening}
        >
          재개
        </Button>
      </div>
      <div className="output">{output}</div>
    </div>
  );
};

const CursorCard = () => {
  const conversationId = useConversationId("s3");
  const sourceRef = useRef<EventSource | null>(null);
  const [prompt, setPrompt] = useState<string>(
    "이벤트 스트리밍을 설명해줘."
  );
  const [messageId, setMessageId] = useState<string | null>(null);
  const [cursor, setCursor] = useState<number>(-1);
  const [output, setOutput] = useState<string>("");
  const [ready, setReady] = useState<boolean>(false);
  const [listening, setListening] = useState<boolean>(false);

  const openReplayStream = (nextMessageId: string, nextCursor: number) => {
    sourceRef.current?.close();
    const url = `/chat/replay?conversationId=${conversationId}&messageId=${nextMessageId}&cursor=${nextCursor}`;
    const source = openEventSource(url, {
      onReplay: ({ data, id }) => {
        setOutput((prev) => prev + data);
        const nextSeq = parseSeqFromEventId(id);
        if (nextSeq !== null) {
          setCursor(nextSeq);
        }
      },
      onChunk: ({ data, id }) => {
        setOutput((prev) => prev + data);
        const nextSeq = parseSeqFromEventId(id);
        if (nextSeq !== null) {
          setCursor(nextSeq);
        }
      },
      onDone: () => {
        setListening(false);
      },
      onError: () => {
        setListening(false);
      },
    });
    sourceRef.current = source;
    setListening(true);
  };

  const invoke = async () => {
    setOutput("");
    setCursor(-1);
    const res = await apiFetch("/chat/write-start", {
      method: "POST",
      body: JSON.stringify({ conversationId, message: prompt }),
    });
    const data = (await res.json()) as {
      conversationId: string;
      messageId: string;
    };
    setMessageId(data.messageId);
    setReady(true);
    openReplayStream(data.messageId, -1);
  };

  const replay = async () => {
    if (!ready || !messageId) return;
    openReplayStream(messageId, cursor);
  };

  const stopReplay = () => {
    sourceRef.current?.close();
    sourceRef.current = null;
    setListening(false);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 3 · 커서 리플레이</span>
      <div className="row">
        <Textarea
          className="prompt-input"
          autoResize={false}
          value={prompt}
          onValueChange={(value) => setPrompt(value)}
        />
      </div>
      <div className="meta">총 응답 길이: {output.length}</div>
      <div className="row">
        <Button onClick={invoke}>요청</Button>
        <Button
          className="secondary"
          onClick={replay}
          disabled={!ready || !messageId || listening}
        >
          재개(커서)
        </Button>
        <Button onClick={stopReplay} disabled={!listening}>
          중지
        </Button>
      </div>
      <div className="output">{output}</div>
    </div>
  );
};

const BufferCard = () => {
  const conversationId = useConversationId("s4");
  const sourceRef = useRef<EventSource | null>(null);
  const [prompt, setPrompt] = useState<string>("레이스 컨디션을 설명해줘.");
  const [messageId, setMessageId] = useState<string | null>(null);
  const [cursor, setCursor] = useState<number>(-1);
  const [output, setOutput] = useState<string>("");
  const [ready, setReady] = useState<boolean>(false);
  const [listening, setListening] = useState<boolean>(false);

  const openBufferStream = (nextMessageId: string, nextCursor: number) => {
    sourceRef.current?.close();
    const url = `/chat/replay-buffer?conversationId=${conversationId}&messageId=${nextMessageId}&cursor=${nextCursor}`;
    const source = openEventSource(url, {
      onReplay: ({ data, id }) => {
        setOutput((prev) => prev + data);
        const nextSeq = parseSeqFromEventId(id);
        if (nextSeq !== null) {
          setCursor(nextSeq);
        }
      },
      onChunk: ({ data, id }) => {
        setOutput((prev) => prev + data);
        const nextSeq = parseSeqFromEventId(id);
        if (nextSeq !== null) {
          setCursor(nextSeq);
        }
      },
      onDone: () => {
        setListening(false);
      },
      onError: () => {
        setListening(false);
      },
    });
    sourceRef.current = source;
    setListening(true);
  };

  const invoke = async () => {
    setOutput("");
    setCursor(-1);
    const res = await apiFetch("/chat/write-start", {
      method: "POST",
      body: JSON.stringify({ conversationId, message: prompt }),
    });
    const data = (await res.json()) as {
      conversationId: string;
      messageId: string;
    };
    setMessageId(data.messageId);
    setReady(true);
    openBufferStream(data.messageId, -1);
  };

  const replay = async () => {
    if (!ready || !messageId) return;
    openBufferStream(messageId, cursor);
  };

  const stopReplay = () => {
    sourceRef.current?.close();
    sourceRef.current = null;
    setListening(false);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 4 · 커서 + 버퍼</span>
      <div className="row">
        <Textarea
          className="prompt-input"
          autoResize={false}
          value={prompt}
          onValueChange={(value) => setPrompt(value)}
        />
      </div>
      <div className="meta">총 응답 길이: {output.length}</div>
      <div className="row">
        <Button onClick={invoke}>요청</Button>
        <Button
          className="secondary"
          onClick={replay}
          disabled={!ready || !messageId || listening}
        >
          재개(버퍼)
        </Button>
        <Button onClick={stopReplay} disabled={!listening}>
          중지
        </Button>
      </div>
      <div className="output">{output}</div>
    </div>
  );
};

const App = () => (
  <div className="page">
    <section className="hero">
      <h1>LLM 스트림 테스트 UI</h1>
      <p>
        Vapor UI + React 기반의 테스트용 대시보드. 네 가지 시나리오를
        호출해 토큰 흐름을 확인한다.
      </p>
    </section>
    <section className="grid">
      <StreamCard />
      <CacheCard />
      <CursorCard />
      <BufferCard />
    </section>
  </div>
);

const root = document.getElementById("root");
if (!root) {
  throw new Error("root element not found");
}
createRoot(root).render(<App />);
