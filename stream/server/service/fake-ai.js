const fs = require("fs");
const path = require("path");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const randomInt = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const readLoremText = () => {
  const filePath = path.join(__dirname, "..", "..", "lorem.txt");
  return fs.readFileSync(filePath, "utf8");
};

class FakeAI {
  constructor({ chunkSize = 12, delayMs = 30, jitterMs = 40 } = {}) {
    this.chunkSize = chunkSize;
    this.delayMs = delayMs;
    this.jitterMs = jitterMs;
    this.text = readLoremText();
  }

  async *stream() {
    for (let i = 0; i < this.text.length; i += this.chunkSize) {
      const jitter = this.jitterMs > 0 ? randomInt(0, this.jitterMs) : 0;
      await sleep(this.delayMs + jitter);
      yield { content: this.text.slice(i, i + this.chunkSize) };
    }
  }

  async invoke() {
    let output = "";
    for (let i = 0; i < this.text.length; i += this.chunkSize) {
      const jitter = this.jitterMs > 0 ? randomInt(0, this.jitterMs) : 0;
      await sleep(this.delayMs + jitter);
      output += this.text.slice(i, i + this.chunkSize);
    }
    return { content: output };
  }
}

module.exports = { FakeAI };
