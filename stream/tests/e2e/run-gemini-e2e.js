const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { setTimeout: sleep } = require("timers/promises");

const BASE_URL = process.env.BASE_URL ?? "http://localhost:8080";
const ITERATIONS = Number.parseInt(process.env.ITERATIONS ?? "3", 10);
const STOP_AFTER_MS = Number.parseInt(process.env.STOP_AFTER_MS ?? "1200", 10);
const RESUME_AFTER_MS = Number.parseInt(
  process.env.RESUME_AFTER_MS ?? "800",
  10
);
const OUTPUT_PATH =
  process.env.OUTPUT_PATH ??
  path.join(process.cwd(), "reports", "e2e-results.jsonl");
const OUTPUT_DIR =
  process.env.OUTPUT_DIR ??
  path.join(process.cwd(), "reports", "e2e-outputs");
const PROMPT_PATH =
  process.env.PROMPT_PATH ??
  path.join(process.cwd(), "tests", "e2e", "prompt.md");
const RUN_ID = process.env.RUN_ID ?? `run-${Date.now()}`;

const MODEL_NAME = process.env.GEMINI_MODEL ?? "gemini-2.5-flash";
const TEMPERATURE = Number.parseFloat(process.env.GEMINI_TEMPERATURE ?? "0");
const TOP_P = Number.parseFloat(process.env.GEMINI_TOP_P ?? "0.1");
const TOP_K = Number.parseInt(process.env.GEMINI_TOP_K ?? "1", 10);

const prompt = fs.readFileSync(PROMPT_PATH, "utf8");

const ensureDir = (filePath) => {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
};

const parseOffsetId = (value) => {
  if (!value) return null;
  const separatorIndex = value.lastIndexOf(":");
  if (separatorIndex <= 0) return null;
  const seq = Number.parseInt(value.slice(separatorIndex + 1), 10);
  if (Number.isNaN(seq)) return null;
  return {
    messageId: value.slice(0, separatorIndex),
    seq,
  };
};

const parseJsonSafely = (value) => {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
};

const readSseStream = async (res, onEvent, signal) => {
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
      const dataLines = [];

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
          const raw = line.slice("data:".length);
          const value = raw.startsWith(" ") ? raw.slice(1) : raw;
          dataLines.push(value);
        }
      }

      if (dataLines.length === 0) continue;
      onEvent({ event, data: dataLines.join("\n"), id });
    }
  }
};

const createCollector = () => {
  const hash = crypto.createHash("sha256");
  const seqSeen = new Set();
  const state = {
    outputParts: [],
    outputLength: 0,
    lastSeq: -1,
    expectedLastSeq: null,
    messageId: null,
    done: false,
    error: null,
  };

  const appendText = (text) => {
    if (!text) return;
    state.outputParts.push(text);
    state.outputLength += text.length;
    hash.update(text, "utf8");
  };

  const markSeq = (seq) => {
    if (seq === null || seq === undefined || seq < 0) return;
    seqSeen.add(seq);
    if (seq > state.lastSeq) state.lastSeq = seq;
  };

  const markRange = (start, end) => {
    if (start > end) return;
    for (let i = start; i <= end; i += 1) {
      markSeq(i);
    }
  };

  const onEvent = (event, replayCursor) => {
    if (event.event === "start") {
      const payload = parseJsonSafely(event.data);
      if (payload && typeof payload.messageId === "string") {
        state.messageId = payload.messageId;
      }
      return;
    }

    if (event.event === "chunk") {
      appendText(event.data);
      const parsed = parseOffsetId(event.id);
      if (parsed) markSeq(parsed.seq);
      return;
    }

    if (event.event === "replay") {
      appendText(event.data);
      const parsed = parseOffsetId(event.id);
      if (parsed) {
        const startSeq = (replayCursor ?? -1) + 1;
        markRange(startSeq, parsed.seq);
      }
      return;
    }

    if (event.event === "done") {
      state.done = true;
      const parsed = parseOffsetId(event.id);
      if (parsed) state.expectedLastSeq = parsed.seq;
      return;
    }

    if (event.event === "error") {
      const payload = parseJsonSafely(event.data);
      state.error =
        payload && typeof payload.message === "string"
          ? payload.message
          : "unknown error";
    }
  };

  const finalize = () => {
    const expectedLastSeq = state.expectedLastSeq;
    const expectedCount =
      expectedLastSeq === null ? null : expectedLastSeq + 1;
    const receivedCount = seqSeen.size;
    const missingCount =
      expectedCount === null ? null : Math.max(0, expectedCount - receivedCount);

    return {
      ...state,
      outputHash: hash.digest("hex"),
      offsetMissingCount: missingCount,
      outputText: state.outputParts.join(""),
    };
  };

  return {
    state,
    onEvent,
    finalize,
  };
};

const fetchJson = async (url, body) => {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
};

const consumeSse = async ({
  url,
  method,
  body,
  collector,
  replayCursor,
  abortAfterMs,
}) => {
  const controller = new AbortController();
  let aborted = false;
  let abortedAt = null;
  let timer = null;

  if (typeof abortAfterMs === "number" && abortAfterMs > 0) {
    timer = setTimeout(() => {
      aborted = true;
      abortedAt = new Date().toISOString();
      controller.abort();
    }, abortAfterMs);
  }

  const startedAt = new Date().toISOString();
  try {
    const res = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body,
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`HTTP ${res.status}: ${text}`);
    }

    await readSseStream(
      res,
      (event) => {
        collector.onEvent(event, replayCursor);
        if (event.event === "done" || event.event === "error") {
          if (timer) clearTimeout(timer);
        }
      },
      controller.signal
    );
  } catch (error) {
    if (
      error instanceof DOMException &&
      error.name === "AbortError" &&
      aborted
    ) {
      return { startedAt, endedAt: new Date().toISOString(), aborted, abortedAt };
    }
    collector.state.error =
      collector.state.error ??
      (error instanceof Error ? error.message : "unknown error");
  } finally {
    if (timer) clearTimeout(timer);
  }

  return {
    startedAt,
    endedAt: new Date().toISOString(),
    aborted,
    abortedAt,
  };
};

const buildConversationId = (scenario, iteration) =>
  `${scenario.toLowerCase()}-${RUN_ID}-${iteration}`;

const writeResult = (record) => {
  ensureDir(OUTPUT_PATH);
  fs.appendFileSync(OUTPUT_PATH, `${JSON.stringify(record)}\n`);
};

const saveOutput = ({ scenario, iteration, messageId, text }) => {
  const safeMessageId = messageId
    ? messageId.replace(/[^a-z0-9-]/gi, "_")
    : "unknown";
  const filename = `${scenario.toLowerCase()}-${RUN_ID}-${iteration}-${safeMessageId}.md`;
  const filePath = path.join(OUTPUT_DIR, filename);
  ensureDir(filePath);
  fs.writeFileSync(filePath, text, "utf8");
  return filePath;
};

const validateMarkdown = (text) => {
  const lines = text.split(/\r?\n/);
  const h1Count = lines.filter((line) => line.startsWith("# ")).length;
  const h2Count = lines.filter((line) => line.startsWith("## ")).length;
  const fenceMatches = text.match(/```/g) ?? [];
  const codeFenceBalanced = fenceMatches.length % 2 === 0;

  const codeMatch = text.match(/```(ts|typescript)\r?\n([\s\S]*?)```/i);
  const codeBlock = codeMatch ? codeMatch[2].trim() : "";
  const codeBlockLines = codeBlock ? codeBlock.split(/\r?\n/).length : 0;

  let replReady = false;
  if (codeBlock) {
    try {
      const ts = require("typescript");
      const result = ts.transpileModule(codeBlock, {
        compilerOptions: { target: ts.ScriptTarget.ES2020 },
      });
      replReady = (result.diagnostics ?? []).length === 0;
    } catch (_error) {
      replReady = false;
    }
  }

  const mdValid =
    h1Count >= 1 &&
    h2Count >= 2 &&
    !!codeBlock &&
    codeBlockLines >= 10 &&
    codeBlockLines <= 20 &&
    codeFenceBalanced;

  return {
    mdValid,
    replReady,
    codeBlockLines,
  };
};

const runScenarioS3 = async (iteration) => {
  const conversationId = buildConversationId("s3", iteration);
  const collector = createCollector();

  const { messageId } = await fetchJson(`${BASE_URL}/chat/write-start`, {
    conversationId,
    message: prompt,
  });

  await consumeSse({
    url: `${BASE_URL}/chat/replay?conversationId=${conversationId}&messageId=${messageId}&cursor=-1&delayDueToNetwork=0&delayDueToHandoff=0`,
    method: "GET",
    collector,
    replayCursor: -1,
    abortAfterMs: STOP_AFTER_MS,
  });

  const resumeCursor = collector.state.lastSeq;
  await sleep(RESUME_AFTER_MS);

  await consumeSse({
    url: `${BASE_URL}/chat/replay?conversationId=${conversationId}&messageId=${messageId}&cursor=${resumeCursor}&delayDueToNetwork=0&delayDueToHandoff=0`,
    method: "GET",
    collector,
    replayCursor: resumeCursor,
    abortAfterMs: null,
  });

  const result = collector.finalize();
  const outputPath = saveOutput({
    scenario: "S3",
    iteration,
    messageId: result.messageId ?? messageId,
    text: result.outputText,
  });
  const validation = validateMarkdown(result.outputText);

  writeResult({
    ts: new Date().toISOString(),
    runId: RUN_ID,
    iteration,
    scenario: "S3",
    modelName: MODEL_NAME,
    temperature: TEMPERATURE,
    topP: TOP_P,
    topK: TOP_K,
    stopAfterMs: STOP_AFTER_MS,
    resumeAfterMs: RESUME_AFTER_MS,
    outputLength: result.outputLength,
    outputHash: result.outputHash,
    outputPath,
    offsetLastSeen: result.lastSeq,
    offsetExpectedLast: result.expectedLastSeq,
    offsetMissingCount: result.offsetMissingCount,
    done: result.done,
    error: result.error,
    mdValid: validation.mdValid,
    replReady: validation.replReady,
    codeBlockLines: validation.codeBlockLines,
  });
};

const runScenarioS4 = async (iteration) => {
  const conversationId = buildConversationId("s4", iteration);
  const collector = createCollector();

  const { messageId } = await fetchJson(`${BASE_URL}/chat/write-start`, {
    conversationId,
    message: prompt,
  });

  await consumeSse({
    url: `${BASE_URL}/chat/replay-buffer?conversationId=${conversationId}&messageId=${messageId}&cursor=-1&delayDueToNetwork=0&delayDueToHandoff=0`,
    method: "GET",
    collector,
    replayCursor: -1,
    abortAfterMs: STOP_AFTER_MS,
  });

  const resumeCursor = collector.state.lastSeq;
  await sleep(RESUME_AFTER_MS);

  await consumeSse({
    url: `${BASE_URL}/chat/replay-buffer?conversationId=${conversationId}&messageId=${messageId}&cursor=${resumeCursor}&delayDueToNetwork=0&delayDueToHandoff=0`,
    method: "GET",
    collector,
    replayCursor: resumeCursor,
    abortAfterMs: null,
  });

  const result = collector.finalize();
  const outputPath = saveOutput({
    scenario: "S4",
    iteration,
    messageId: result.messageId ?? messageId,
    text: result.outputText,
  });
  const validation = validateMarkdown(result.outputText);

  writeResult({
    ts: new Date().toISOString(),
    runId: RUN_ID,
    iteration,
    scenario: "S4",
    modelName: MODEL_NAME,
    temperature: TEMPERATURE,
    topP: TOP_P,
    topK: TOP_K,
    stopAfterMs: STOP_AFTER_MS,
    resumeAfterMs: RESUME_AFTER_MS,
    outputLength: result.outputLength,
    outputHash: result.outputHash,
    outputPath,
    offsetLastSeen: result.lastSeq,
    offsetExpectedLast: result.expectedLastSeq,
    offsetMissingCount: result.offsetMissingCount,
    done: result.done,
    error: result.error,
    mdValid: validation.mdValid,
    replReady: validation.replReady,
    codeBlockLines: validation.codeBlockLines,
  });
};

const run = async () => {
  for (let i = 1; i <= ITERATIONS; i += 1) {
    await runScenarioS3(i);
    await runScenarioS4(i);
  }
};

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
