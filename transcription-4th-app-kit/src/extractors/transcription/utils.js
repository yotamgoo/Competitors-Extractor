export function nowIso() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function firstText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text && text.toLowerCase() !== "null" && text.toLowerCase() !== "none") {
      return text;
    }
  }
  return "";
}

export function toInt(value, fallback = 0) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function excerpt(value, limit = 500) {
  return String(value ?? "").trim().slice(0, limit);
}

export function normalizeStatus(value, fallback = "pending") {
  const text = String(value ?? "").trim().toLowerCase();
  if (text === "running" || text === "done" || text === "failed" || text === "pending") {
    return text;
  }
  return fallback;
}
