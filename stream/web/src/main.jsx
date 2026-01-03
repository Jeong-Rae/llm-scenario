import { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { Button, TextInput } from "@vapor-ui/core";
import "@vapor-ui/core/styles.css";
import "./styles.css";

const apiFetch = (path, options) =>
  fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

const useIds = (prefix) => {
  const now = useMemo(() => Date.now().toString(36).slice(-6), []);
  return {
    conversationId: `${prefix}-c-${now}`,
    messageId: `${prefix}-m-${now}`,
  };
};

const StreamCard = () => {
  const ids = useIds("s1");
  const [prompt, setPrompt] = useState("간단한 인사말을 작성해줘.");
  const [output, setOutput] = useState("");
  const [busy, setBusy] = useState(false);

  const startStream = async () => {
    setBusy(true);
    setOutput("");
    const res = await apiFetch("/scenario1/stream", {
      method: "POST",
      body: JSON.stringify({ ...ids, prompt }),
    });
    if (!res.body) {
      setBusy(false);
      return;
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split("\n\n");
      buffer = parts.pop() ?? "";
      for (const part of parts) {
        const line = part.replace(/^data:\s?/, "");
        setOutput((prev) => prev + line);
      }
    }
    setBusy(false);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 1 · POST + 스트리밍</span>
      <div className="row">
        <TextInput value={prompt} onChange={(e) => setPrompt(e.target.value)} />
      </div>
      <Button onClick={startStream} disabled={busy}>
        {busy ? "스트리밍 중..." : "스트림 시작"}
      </Button>
      <div className="output">{output}</div>
    </div>
  );
};

const CacheCard = () => {
  const ids = useIds("s2");
  const [prompt, setPrompt] = useState("영화 장면을 요약해줘.");
  const [output, setOutput] = useState("");
  const [busy, setBusy] = useState(false);
  const [polling, setPolling] = useState(false);
  const [started, setStarted] = useState(false);

  const invoke = async () => {
    setBusy(true);
    setOutput("");
    await apiFetch("/scenario2/write", {
      method: "POST",
      body: JSON.stringify({ ...ids, prompt }),
    });
    setBusy(false);
    setStarted(true);
    setPolling(true);
  };

  const poll = async () => {
    if (!polling || !started) return;
    const res = await apiFetch(
      `/scenario2/read?conversationId=${ids.conversationId}&messageId=${ids.messageId}`
    );
    const data = await res.json();
    if (data.tokens?.length) {
      setOutput((prev) => prev + data.tokens.join(""));
    }
    if (data.done) {
      setPolling(false);
    }
  };

  useEffect(() => {
    if (!polling || !started) return;
    const id = setInterval(poll, 500);
    return () => clearInterval(id);
  }, [polling, started]);

  useEffect(() => {
    setPolling(false);
    setStarted(false);
  }, []);

  return (
    <div className="card">
      <span className="pill">시나리오 2 · POST(쓰기) + GET(읽기)</span>
      <div className="row">
        <TextInput value={prompt} onChange={(e) => setPrompt(e.target.value)} />
      </div>
      <Button className="secondary" onClick={invoke} disabled={busy}>
        {busy ? "요청 중..." : "요청"}
      </Button>
      <div className="output">{output}</div>
    </div>
  );
};

const CursorCard = () => {
  const ids = useIds("s3");
  const [prompt, setPrompt] = useState("이벤트 스트리밍을 설명해줘.");
  const [cursor, setCursor] = useState(0);
  const [output, setOutput] = useState("");
  const [ready, setReady] = useState(false);

  const invoke = async () => {
    setOutput("");
    setCursor(0);
    await apiFetch("/scenario2/write", {
      method: "POST",
      body: JSON.stringify({ ...ids, prompt }),
    });
    setReady(true);
  };

  const replay = async () => {
    if (!ready) return;
    const res = await apiFetch(
      `/scenario3/read-with-cursor?conversationId=${ids.conversationId}&messageId=${ids.messageId}&cursor=${cursor}`
    );
    const data = await res.json();
    setOutput((prev) => prev + (data.tokens ?? []).join(""));
    setCursor(data.nextCursor ?? cursor);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 3 · 커서 리플레이</span>
      <div className="row">
        <TextInput value={prompt} onChange={(e) => setPrompt(e.target.value)} />
      </div>
      <div className="row">
        <Button onClick={invoke}>요청</Button>
        <Button className="secondary" onClick={replay}>
          커서부터 다시 받기
        </Button>
      </div>
      <div className="output">{output}</div>
    </div>
  );
};

const BufferCard = () => {
  const ids = useIds("s4");
  const [prompt, setPrompt] = useState("레이스 컨디션을 설명해줘.");
  const [cursor, setCursor] = useState(0);
  const [output, setOutput] = useState("");
  const [ready, setReady] = useState(false);

  const invoke = async () => {
    setOutput("");
    setCursor(0);
    await apiFetch("/scenario2/write", {
      method: "POST",
      body: JSON.stringify({ ...ids, prompt }),
    });
    setReady(true);
  };

  const replay = async () => {
    if (!ready) return;
    const res = await apiFetch(
      `/scenario4/read-with-buffer?conversationId=${ids.conversationId}&messageId=${ids.messageId}&cursor=${cursor}`
    );
    const data = await res.json();
    setOutput((prev) => prev + (data.tokens ?? []).join(""));
    setCursor(data.nextCursor ?? cursor);
  };

  return (
    <div className="card">
      <span className="pill">시나리오 4 · 커서 + 버퍼</span>
      <div className="row">
        <TextInput value={prompt} onChange={(e) => setPrompt(e.target.value)} />
      </div>
      <div className="row">
        <Button onClick={invoke}>요청</Button>
        <Button className="secondary" onClick={replay}>
          버퍼 포함 재수신
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
createRoot(root).render(<App />);
