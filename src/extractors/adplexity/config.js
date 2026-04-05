import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentFile = fileURLToPath(import.meta.url);
const rootDir = path.resolve(path.dirname(currentFile), "../../..");

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

function envInt(name, fallback) {
  const parsed = Number.parseInt(envText(name, String(fallback)), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function envIntAny(names, fallback) {
  const parsed = Number.parseInt(envTextAny(names, String(fallback)), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function requiredAny(names) {
  const value = envTextAny(names);
  if (!value) {
    throw new Error(`Missing required environment variable: ${names.join(" or ")}`);
  }
  return value;
}

export const config = {
  port: envInt("PORT", 8080),
  adplexity: {
    email: envTextAny(["ADPLEXITY_EMAIL"]),
    password: envTextAny(["ADPLEXITY_PASSWORD"])
  },
  airtable: {
    token: envTextAny(["ADPLEXITY_AIRTABLE_PAT", "AIRTABLE_PAT"]),
    baseId: envTextAny(["ADPLEXITY_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]),
    competitorsTable: envTextAny(
      ["ADPLEXITY_AIRTABLE_COMPETITORS_TABLE", "AIRTABLE_COMPETITORS_TABLE"],
      "Competitors"
    ),
    adsTable: envTextAny(["ADPLEXITY_AIRTABLE_ADS_TABLE", "AIRTABLE_ADS_TABLE"], "Ads"),
    checkpointsTable: envTextAny(
      ["ADPLEXITY_AIRTABLE_CHECKPOINTS_TABLE", "AIRTABLE_CHECKPOINTS_TABLE"],
      "Adplexity Checkpoints"
    ),
    competitorsActiveField: envTextAny(
      ["ADPLEXITY_AIRTABLE_COMPETITORS_ACTIVE_FIELD", "AIRTABLE_COMPETITORS_ACTIVE_FIELD"],
      "Active"
    ),
    competitorsNameField: envTextAny(
      ["ADPLEXITY_AIRTABLE_COMPETITORS_NAME_FIELD", "AIRTABLE_COMPETITORS_NAME_FIELD"],
      "Name"
    ),
    competitorsAdplexityField: envTextAny(
      ["ADPLEXITY_AIRTABLE_COMPETITORS_ADPLEXITY_FIELD", "AIRTABLE_COMPETITORS_ADPLEXITY_FIELD"],
      "AdPlexity Report ID"
    )
  },
  runtime: {
    chunkMaxMs: envIntAny(["ADPLEXITY_CHUNK_MAX_MS", "CHUNK_MAX_MS"], 300000),
    chunkMaxCheckpoints: envIntAny(["ADPLEXITY_CHUNK_MAX_CHECKPOINTS", "CHUNK_MAX_CHECKPOINTS"], 10),
    retryLimit: envIntAny(["ADPLEXITY_CHECKPOINT_RETRY_LIMIT", "CHECKPOINT_RETRY_LIMIT"], 3),
    staleMinutes: envIntAny(["ADPLEXITY_CHECKPOINT_STALE_MINUTES", "CHECKPOINT_STALE_MINUTES"], 10)
  }
};

export function ensureAdplexityConfigured() {
  requiredAny(["ADPLEXITY_EMAIL"]);
  requiredAny(["ADPLEXITY_PASSWORD"]);
}

export function ensureAirtableConfigured() {
  requiredAny(["ADPLEXITY_AIRTABLE_PAT", "AIRTABLE_PAT"]);
  requiredAny(["ADPLEXITY_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]);
}
