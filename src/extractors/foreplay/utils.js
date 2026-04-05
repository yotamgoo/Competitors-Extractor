export function nowIso() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
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
