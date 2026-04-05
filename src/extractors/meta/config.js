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

function normalizeMedia(text) {
  const value = String(text ?? "").trim().toLowerCase();
  if (value === "image" || value === "video" || value === "both") {
    return value;
  }
  return "both";
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
  airtable: {
    token: envTextAny(["META_AIRTABLE_PAT", "AIRTABLE_PAT"]),
    baseId: envTextAny(["META_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]),
    competitorsTable: envTextAny(["META_AIRTABLE_COMPETITORS_TABLE", "AIRTABLE_COMPETITORS_TABLE"], "Competitors"),
    adsTable: envTextAny(["META_AIRTABLE_ADS_TABLE", "AIRTABLE_ADS_TABLE"], "Ads"),
    checkpointsTable: envTextAny(
      ["META_AIRTABLE_CHECKPOINTS_TABLE", "AIRTABLE_CHECKPOINTS_TABLE"],
      "Meta Checkpoints"
    ),
    competitorsActiveField: envTextAny(
      ["META_AIRTABLE_COMPETITORS_ACTIVE_FIELD", "AIRTABLE_COMPETITORS_ACTIVE_FIELD"],
      "Active"
    ),
    competitorsNameField: envTextAny(
      ["META_AIRTABLE_COMPETITORS_NAME_FIELD", "AIRTABLE_COMPETITORS_NAME_FIELD"],
      "Name"
    ),
    competitorsMetaPageField: envTextAny(
      ["META_AIRTABLE_COMPETITORS_META_PAGE_FIELD", "AIRTABLE_COMPETITORS_META_PAGE_FIELD"],
      "Meta Page ID"
    )
  },
  runtime: {
    chunkMaxMs: envIntAny(["META_CHUNK_MAX_MS", "CHUNK_MAX_MS"], 300000),
    chunkMaxCheckpoints: envIntAny(["META_CHUNK_MAX_CHECKPOINTS", "CHUNK_MAX_CHECKPOINTS"], 10),
    retryLimit: envIntAny(["META_CHECKPOINT_RETRY_LIMIT", "CHECKPOINT_RETRY_LIMIT"], 3),
    staleMinutes: envIntAny(["META_CHECKPOINT_STALE_MINUTES", "CHECKPOINT_STALE_MINUTES"], 10),
    minDays: envInt("META_MIN_DAYS", 30),
    media: normalizeMedia(envText("META_MEDIA", "both")),
    maxAdsPerPage: envInt("META_MAX_ADS_PER_PAGE", 100),
    sliceMaxAds: envInt("META_SLICE_MAX_ADS", 30)
  },
  metaApi: {
    url: envText("META_GRAPHQL_API_URL", "https://www.facebook.com/api/graphql/"),
    formTemplate: envText("META_GRAPHQL_FORM_TEMPLATE"),
    docId: envText("META_GRAPHQL_DOC_ID", "25987067537594875"),
    friendlyName: envText("META_GRAPHQL_FRIENDLY_NAME", "AdLibrarySearchPaginationQuery"),
    xAsbdId: envText("META_X_ASBD_ID", "359341"),
    cookie: envText("META_COOKIE"),
    fbDtsg: envText("META_FB_DTSG"),
    lsd: envText("META_X_FB_LSD"),
    acceptLanguage: envText("META_ACCEPT_LANGUAGE", "en-GB,en-US;q=0.9,en;q=0.8,he;q=0.7"),
    userAgent: envText(
      "META_USER_AGENT",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
    timeoutMs: envInt("META_REQUEST_TIMEOUT_MS", 90000)
  }
};

export function ensureAirtableConfigured() {
  requiredAny(["META_AIRTABLE_PAT", "AIRTABLE_PAT"]);
  requiredAny(["META_AIRTABLE_BASE_ID", "AIRTABLE_BASE_ID"]);
}
