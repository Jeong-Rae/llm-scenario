import fs from "fs";
import path from "path";

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));
const randomInt = (min: number, max: number) =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const readLoremText = (): string => {
  const filePath = path.join(__dirname, "..", "..", "lorem.txt");
  return fs.readFileSync(filePath, "utf8");
};

type FakeAIOptions = {
  chunkSize?: number;
  delayMs?: number;
  jitterMs?: number;
};

class FakeAI {
  private chunkSize: number;
  private delayMs: number;
  private jitterMs: number;
  private text: string;

  constructor({ chunkSize = 12, delayMs = 30, jitterMs = 40 }: FakeAIOptions = {}) {
    this.chunkSize = chunkSize;
    this.delayMs = delayMs;
    this.jitterMs = jitterMs;
    this.text = readLoremText();
  }

  async *stream(): AsyncGenerator<{ content: string }, void, void> {
    for (let i = 0; i < this.text.length; i += this.chunkSize) {
      const jitter = this.jitterMs > 0 ? randomInt(0, this.jitterMs) : 0;
      await sleep(this.delayMs + jitter);
      yield { content: this.text.slice(i, i + this.chunkSize) };
    }
  }

  async invoke(): Promise<{ content: string }> {
    let output = "";
    for (let i = 0; i < this.text.length; i += this.chunkSize) {
      const jitter = this.jitterMs > 0 ? randomInt(0, this.jitterMs) : 0;
      await sleep(this.delayMs + jitter);
      output += this.text.slice(i, i + this.chunkSize);
    }
    return { content: output };
  }
}

export { FakeAI };
