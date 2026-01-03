const express = require("express");
const { registerRoutes } = require("./controller/llm");

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  console.log(`[요청] ${req.method} ${req.url}`);
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    res.status(204).end();
    return;
  }
  next();
});

registerRoutes(app);

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`[서버] http://localhost:${port} 에서 대기 중`);
});
