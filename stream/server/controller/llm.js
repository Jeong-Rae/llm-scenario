const { LLMService } = require("../service/llm");

// 기술 블로그 예시 컨트롤러: express 스타일 핸들러.
const service = new LLMService();

const postStream = async (req, res) => {
  const { conversationId, messageId, prompt } = req.body;
  let emitter;
  try {
    emitter = await service.streamAndPersist({
      conversationId,
      messageId,
      prompt,
    });
  } catch (error) {
    console.error("[서버] 스트림 요청 실패", error);
    res.status(500).json({ error: "LLM 요청 실패" });
    return;
  }

  res.setHeader("Content-Type", "text/event-stream");
  emitter.on("token", (token) => {
    res.write(`data: ${token}\n\n`);
  });
  emitter.on("done", () => res.end());
  emitter.on("error", (error) => {
    console.error("[서버] 스트림 처리 오류", error);
    res.write(`data: 오류가 발생했습니다.\n\n`);
    res.end();
  });
};

const postInvokeCache = async (req, res) => {
  const { conversationId, messageId, prompt } = req.body;
  try {
    const result = await service.invokeAndCache({
      conversationId,
      messageId,
      prompt,
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error("[서버] invoke 요청 실패", error);
    res.status(500).json({ error: "LLM 요청 실패" });
  }
};

const getCachedTokens = (req, res) => {
  const { conversationId, messageId } = req.query;
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Pragma", "no-cache");
  const result = service.getCachedTokens({ conversationId, messageId });
  res.json(result);
};

const getReplayWithCursor = (req, res) => {
  const { conversationId, messageId, cursor } = req.query;
  service
    .getTokensWithCursor({
      conversationId,
      messageId,
      cursor: Number(cursor ?? 0),
    })
    .then((result) => res.json(result));
};

const getReplayWithBuffer = (req, res) => {
  const { conversationId, messageId, cursor } = req.query;
  service
    .getTokensWithCursorAndBuffer({
      conversationId,
      messageId,
      cursor: Number(cursor ?? 0),
    })
    .then((result) => res.json(result));
};

const registerRoutes = (app) => {
  app.post("/llm/stream", postStream);
  app.post("/llm/invoke-cache", postInvokeCache);
  app.get("/llm/cache", getCachedTokens);
  app.get("/llm/replay", getReplayWithCursor);
  app.get("/llm/replay-buffer", getReplayWithBuffer);
};

module.exports = {
  registerRoutes,
  postStream,
  postInvokeCache,
  getCachedTokens,
  getReplayWithCursor,
  getReplayWithBuffer,
};
