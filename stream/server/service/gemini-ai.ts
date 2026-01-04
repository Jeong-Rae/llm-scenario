import { ChatGoogleGenerativeAI } from "@langchain/google-genai";
import { HumanMessage } from "@langchain/core/messages";

const extractText = (content: unknown): string => {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((part) => {
        if (typeof part === "string") return part;
        if (
          part &&
          typeof part === "object" &&
          "text" in part &&
          typeof (part as { text?: unknown }).text === "string"
        ) {
          return (part as { text: string }).text;
        }
        return "";
      })
      .join("");
  }
  return "";
};

type GeminiAIOptions = {
  userMessage?: string;
  modelName: string;
  temperature: number;
  topP: number;
  topK: number;
  maxOutputTokens?: number;
};

class GeminiAI {
  private model: ChatGoogleGenerativeAI;
  private prompt: string;

  constructor({
    userMessage,
    modelName,
    temperature,
    topP,
    topK,
    maxOutputTokens,
  }: GeminiAIOptions) {
    const apiKey = process.env.GOOGLE_API_KEY ?? process.env.GEMINI_API_KEY;
    if (!apiKey) {
      throw new Error("GOOGLE_API_KEY is required for Gemini streaming.");
    }

    this.prompt = userMessage ?? "";
    this.model = new ChatGoogleGenerativeAI({
      apiKey,
      modelName,
      temperature,
      topP,
      topK,
      maxOutputTokens,
      streaming: true,
      streamUsage: false,
    });
  }

  async *stream(): AsyncGenerator<{ content: string }, void, void> {
    const stream = await this.model.stream([
      new HumanMessage({ content: this.prompt }),
    ]);
    for await (const chunk of stream) {
      const text = extractText(chunk.content);
      if (text) {
        yield { content: text };
      }
    }
  }
}

export { GeminiAI };
