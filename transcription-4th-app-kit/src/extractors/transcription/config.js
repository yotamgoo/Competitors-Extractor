import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentFile = fileURLToPath(import.meta.url);
const rootDir = path.resolve(path.dirname(currentFile), "../../..");
const envKeysLoadedFromFile = new Set();

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const content = fs.readFileSync(filePath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#") || !line.includes("=")) {
      continue;
    }

    const [keyPart, ...valueParts] = line.split("=");
    const key = keyPart.trim();
    const value = valueParts.join("=").trim().replace(/^['"]|['"]$/g, "");
    if (key && !(key in process.env)) {
      process.env[key] = value;
      envKeysLoadedFromFile.add(key);
    }
  }
}

loadEnvFile(path.join(rootDir, ".env"));

function envText(name, fallback = "") {
  return String(process.env[name] ?? fallback).trim();
}

function envTextAny(names, fallback = "") {
  for (const name of names) {
    const value = envText(name);
    if (value) {
      return value;
    }
  }
  return String(fallback).trim();
}

function envIntAny(names, fallback) {
  const parsed = Number.parseInt(envTextAny(names, String(fallback)), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function envMatchAny(names) {
  for (const name of names) {
    const value = envText(name);
    if (value) {
      return {
        name,
        value,
        source: envKeysLoadedFromFile.has(name) ? ".env file" : "runtime secret"
      };
    }
  }

  return {
    name: "",
    value: "",
    source: ""
  };
}

function requiredAny(names) {
  const value = envTextAny(names);
  if (!value) {
    throw new Error(`Missing required environment variable: ${names.join(" or ")}`);
  }
  return value;
}

const geminiApiKey = envMatchAny(["GEMINI_API_KEY", "GOOGLE_API_KEY"]);

export const config = {
  port: envIntAny(["PORT"], 8080),
  airtable: {
    token: envTextAny(["TRANSCRIPTION_AIRTABLE_PAT", "AIRTABLE_PAT"]),
    baseId: envTextAny(["TRANSCRIPTION_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]),
    ads: {
      table: envTextAny(["TRANSCRIPTION_AIRTABLE_ADS_TABLE", "AIRTABLE_ADS_TABLE"], "Ads"),
      view: envTextAny(["TRANSCRIPTION_AIRTABLE_ADS_VIEW"], ""),
      adIdField: envTextAny(["TRANSCRIPTION_AIRTABLE_ADS_AD_ID_FIELD"], "Ad Id"),
      attachmentField: envTextAny(["TRANSCRIPTION_AIRTABLE_ADS_ATTACHMENT_FIELD"], "Ad File"),
      transcriptField: envTextAny(["TRANSCRIPTION_AIRTABLE_ADS_TRANSCRIPT_FIELD"], "Video Transcript")
    },
    queue: {
      table: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_TABLE"], "Transcriptions"),
      view: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_VIEW"], ""),
      adIdField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_AD_ID_FIELD"], ""),
      adsRecordIdField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_ADS_RECORD_ID_FIELD"], ""),
      adLinkField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_AD_LINK_FIELD"], "Ad"),
      transcriptField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_TRANSCRIPT_FIELD"], "Video Transcript"),
      statusField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_STATUS_FIELD"], "Transcript Status"),
      errorField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_ERROR_FIELD"], "Transcript Error"),
      updatedAtField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_UPDATED_AT_FIELD"], "Transcript Updated At"),
      attemptCountField: envTextAny(
        ["TRANSCRIPTION_AIRTABLE_QUEUE_ATTEMPT_COUNT_FIELD"],
        "Transcript Attempt Count"
      ),
      heartbeatField: envTextAny(
        ["TRANSCRIPTION_AIRTABLE_QUEUE_HEARTBEAT_FIELD"],
        "Transcript Heartbeat At"
      ),
      languageField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_LANGUAGE_FIELD"], "Transcript Language"),
      seededAtField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_SEEDED_AT_FIELD"], ""),
      completedAtField: envTextAny(["TRANSCRIPTION_AIRTABLE_QUEUE_COMPLETED_AT_FIELD"], "")
    }
  },
  runtime: {
    chunkMaxMs: envIntAny(["TRANSCRIPTION_CHUNK_MAX_MS"], 180000),
    chunkMaxRecords: envIntAny(["TRANSCRIPTION_CHUNK_MAX_RECORDS"], 10),
    retryLimit: envIntAny(["TRANSCRIPTION_RETRY_LIMIT"], 3),
    staleMinutes: envIntAny(["TRANSCRIPTION_STALE_MINUTES"], 15),
    maxInlineBytes: envIntAny(["TRANSCRIPTION_MAX_INLINE_BYTES"], 15000000)
  },
  gemini: {
    apiKey: geminiApiKey.value,
    apiKeyName: geminiApiKey.name,
    apiKeySource: geminiApiKey.source,
    model: envText("GEMINI_MODEL", "gemini-2.5-flash"),
    imageModel: envText("GEMINI_IMAGE_MODEL", "gemini-2.0-flash"),
    apiUrl: envText("GEMINI_API_URL", "https://generativelanguage.googleapis.com/v1beta"),
    language: envText("TRANSCRIPTION_LANGUAGE"),
    prompt: envText(
      "TRANSCRIPTION_PROMPT",
      "Transcribe all spoken words from this video verbatim from start to finish. Do not summarize, shorten, paraphrase, or describe the video. Return only the complete transcript text in reading order. If there is no speech, return exactly NO_SPEECH."
    ),
    imagePrompt: envText(
      "IMAGE_DESCRIPTION_PROMPT",
      "Describe this ad image in one short paragraph for marketing classification. Mention the core visual, product or offer, any visible text or CTA, audience cues, and the likely creative angle. Keep it concise, plain, and useful for later vertical filtering. Return only the paragraph."
    ),
    maxOutputTokens: envIntAny(["GEMINI_MAX_OUTPUT_TOKENS"], 8192),
    temperature: Number.parseFloat(envText("GEMINI_TEMPERATURE", "0.1")) || 0.1,
    timeoutMs: envIntAny(["GEMINI_REQUEST_TIMEOUT_MS"], 120000)
  }
};

export function ensureAirtableConfigured() {
  requiredAny(["TRANSCRIPTION_AIRTABLE_PAT", "AIRTABLE_PAT"]);
  requiredAny(["TRANSCRIPTION_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]);
}

export function ensureGeminiConfigured() {
  requiredAny(["GEMINI_API_KEY", "GOOGLE_API_KEY"]);
}
