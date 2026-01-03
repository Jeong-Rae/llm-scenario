import express, { NextFunction, Request, Response } from "express";
import { registerRoutes } from "./controller/llm";

const app = express();
app.use(express.json());
app.use((req: Request, res: Response, next: NextFunction) => {
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

const parsedPort = Number(process.env.PORT);
const port = Number.isFinite(parsedPort) ? parsedPort : 8080;
app.listen(port, () => {
  console.log(`[서버] http://localhost:${port} 에서 대기 중`);
});
