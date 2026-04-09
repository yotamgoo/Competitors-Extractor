import { GoogleGenAI } from "@google/genai";

import { ensureGeminiConfigured } from "./config.js";

function inferMimeType(attachment) {
  const type = String(attachment?.type || "").trim();
  if (type) {
    return type;
  }
  return "video/mp4";
}

function extractText(response) {
  const text = String(response?.text || "").trim();
  if (text) {
    return text;
  }

  const candidates = Array.isArray(response?.candidates) ? response.candidates : [];
  const parts = candidates.flatMap((candidate) =>
    Array.isArray(candidate?.content?.parts) ? candidate.content.parts : []
  );
  const joined = parts
    .map((part) => String(part?.text || "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();

  if (joined) {
    return joined;
  }

  const blockReason = String(response?.promptFeedback?.blockReason || "").trim();
  if (blockReason) {
    throw new Error(`Gemini blocked the request: ${blockReason}`);
  }

  throw new Error("Gemini returned no transcript text.");
}

export class GeminiClient {
  constructor(config) {
    this.config = config;
    this.client = new GoogleGenAI({
      apiKey: this.config.gemini.apiKey || ""
    });
  }

  ensureConfigured() {
    ensureGeminiConfigured();
  }

  async transcribeAttachment(attachment, options = {}) {
    this.ensureConfigured();

    if (!attachment?.url) {
      throw new Error("Attachment URL is missing.");
    }

    if (attachment.size && attachment.size > this.config.runtime.maxInlineBytes) {
      throw new Error(
        `Attachment is ${attachment.size} bytes and exceeds the ${this.config.runtime.maxInlineBytes} byte inline limit.`
      );
    }

    const download = await fetch(attachment.url, {
      signal: AbortSignal.timeout(this.config.gemini.timeoutMs)
    });
    if (!download.ok) {
      throw new Error(`Attachment download failed (${download.status}).`);
    }

    const buffer = Buffer.from(await download.arrayBuffer());
    if (!buffer.byteLength) {
      throw new Error("Attachment download returned an empty file.");
    }
    if (buffer.byteLength > this.config.runtime.maxInlineBytes) {
      throw new Error(
        `Attachment is ${buffer.byteLength} bytes and exceeds the ${this.config.runtime.maxInlineBytes} byte inline limit.`
      );
    }

    const language = String(options.language || this.config.gemini.language || "").trim();
    const prompt = language
      ? `${this.config.gemini.prompt}\n\nLanguage hint: ${language}.`
      : this.config.gemini.prompt;

    try {
      const response = await this.client.models.generateContent({
        model: this.config.gemini.model,
        contents: [
          {
            inlineData: {
              mimeType: inferMimeType(attachment),
              data: buffer.toString("base64")
            }
          },
          {
            text: prompt
          }
        ]
      });

      const text = extractText(response);
      return {
        text,
        model: this.config.gemini.model,
        bytes: buffer.byteLength
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Gemini request failed: ${message}`);
    }
  }
}
