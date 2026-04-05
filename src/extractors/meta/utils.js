export function nowIso() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function normalizePageId(value) {
  return String(value ?? "").replace(/[^\d]/g, "").trim();
}

export function hashText(value) {
  const text = String(value ?? "");
  let hash = 2166136261;
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(36);
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function coerceLineList(value) {
  return String(value ?? "")
    .replaceAll(",", "\n")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function coerceIntList(value) {
  return uniqueNumbers(
    coerceLineList(value)
      .map((item) => Number.parseInt(String(item), 10))
      .filter((item) => Number.isFinite(item) && item > 0)
  );
}

export function uniqueStrings(values) {
  const seen = new Set();
  const output = [];
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text || seen.has(text)) {
      continue;
    }
    seen.add(text);
    output.push(text);
  }
  return output;
}

export function uniqueNumbers(values) {
  const seen = new Set();
  const output = [];
  for (const value of values) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      continue;
    }
    const normalized = Math.trunc(numeric);
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

export function pickFirstText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text && text.toLowerCase() !== "null" && text.toLowerCase() !== "none") {
      return text;
    }
  }
  return "";
}

export function cleanHtmlText(value) {
  return String(value ?? "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeDateOnly(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return text;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.valueOf())) {
    return "";
  }
  return parsed.toISOString().slice(0, 10);
}

export function parseDateBucketTimestamp(dateText) {
  const match = /\d{10,}/.exec(String(dateText ?? ""));
  return match ? Number.parseInt(match[0], 10) : null;
}

export function lookbackStartMs(months) {
  return Date.now() - Math.max(1, months) * 30 * 24 * 60 * 60 * 1000;
}

export function toUtcDayRange(dateText) {
  const normalized = normalizeDateOnly(dateText);
  if (!normalized) {
    throw new Error(`Invalid bucket date: ${dateText}`);
  }
  const start = Date.parse(`${normalized}T00:00:00.000Z`);
  const end = start + 24 * 60 * 60 * 1000 - 1;
  return {
    normalized,
    start,
    end
  };
}

export function normalizeDateTime(value) {
  if (value === null || value === undefined || value === "" || value === 0) {
    return null;
  }
  if (value instanceof Date) {
    return value.toISOString().replace(/\.\d{3}Z$/, "Z");
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const timestamp = value > 1_000_000_000_000 ? value : value * 1000;
    return new Date(timestamp).toISOString().replace(/\.\d{3}Z$/, "Z");
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.valueOf())) {
    return text;
  }
  return parsed.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function normalizeDaysRunning(days, options = {}) {
  if (days !== null && days !== undefined && String(days).trim() !== "") {
    const numeric = Number(days);
    if (Number.isFinite(numeric)) {
      return Math.trunc(numeric);
    }
  }

  const firstIso = normalizeDateTime(options.firstSeen);
  const lastIso = normalizeDateTime(options.lastSeen) || nowIso();
  if (!firstIso) {
    return null;
  }

  const first = new Date(firstIso);
  const last = new Date(lastIso);
  if (Number.isNaN(first.valueOf()) || Number.isNaN(last.valueOf())) {
    return null;
  }

  const deltaMs = last.valueOf() - first.valueOf();
  return Math.max(Math.floor(deltaMs / 86400000), 0);
}

export function normalizePlatformList(value) {
  const items = Array.isArray(value) ? value : String(value ?? "").split(",");
  const seen = new Set();
  const output = [];
  for (const item of items) {
    const normalized = String(item ?? "")
      .trim()
      .toLowerCase()
      .replaceAll("-", " ")
      .replaceAll("/", " ")
      .split(/\s+/)
      .filter(Boolean)
      .join("_");
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

export function normalizeCountryList(value) {
  const items = Array.isArray(value) ? value : String(value ?? "").split(",");
  const seen = new Set();
  const output = [];
  for (const item of items) {
    const normalized = String(item ?? "").trim().toUpperCase();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

export function displayBrandFromUrl(url) {
  if (!url) {
    return "";
  }

  try {
    const parsed = new URL(url);
    let host = parsed.hostname.toLowerCase();
    if (host.startsWith("www.")) {
      host = host.slice(4);
    }

    if (!host) {
      return "";
    }

    const rawParts = host.split(".").filter(Boolean);
    if (!rawParts.length) {
      return "";
    }

    let core = rawParts.length >= 2 ? rawParts[rawParts.length - 2] : rawParts[0];
    if (["l", "m", "app", "go", "click"].includes(core) && rawParts.length >= 3) {
      core = rawParts[rawParts.length - 3];
    }

    return core
      .replaceAll("-", " ")
      .replaceAll("_", " ")
      .replace(/\b\w/g, (match) => match.toUpperCase());
  } catch {
    return "";
  }
}

export function formatPlatformLabel(platform) {
  return String(platform ?? "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

export function serializeJson(value) {
  return JSON.stringify(value ?? {});
}

export function parseJsonObject(value) {
  if (!value) {
    return null;
  }
  try {
    const parsed = JSON.parse(String(value));
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

export function isTruthyLike(value) {
  if (value === undefined || value === null || value === "") {
    return true;
  }
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  const text = String(value).trim().toLowerCase();
  if (!text) {
    return true;
  }
  return !["false", "0", "no", "off", "inactive", "disabled"].includes(text);
}
