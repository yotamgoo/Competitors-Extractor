import fs from "node:fs";

function findHeader(headers, name) {
  const needle = String(name).toLowerCase();
  return (headers || []).find((header) => String(header.name).toLowerCase() === needle)?.value || "";
}

function joinHarCookies(cookies) {
  if (!Array.isArray(cookies) || !cookies.length) {
    return "";
  }
  return cookies
    .map((cookie) => {
      const name = String(cookie?.name ?? "").trim();
      const value = String(cookie?.value ?? "").trim();
      return name && value ? `${name}=${value}` : "";
    })
    .filter(Boolean)
    .join("; ");
}

function findAdLibraryEntry(har) {
  return (har?.log?.entries || []).find((entry) => {
    const url = String(entry?.request?.url ?? "");
    const postData = String(entry?.request?.postData?.text ?? "");
    return url.includes("/api/graphql/") && postData.includes("AdLibrarySearchPaginationQuery");
  });
}

function tryParseHar(text) {
  try {
    const parsed = JSON.parse(text);
    const entry = findAdLibraryEntry(parsed);
    if (!entry) {
      return null;
    }
    return {
      sourceType: "har",
      formTemplate: String(entry.request?.postData?.text ?? "").trim(),
      cookie: findHeader(entry.request?.headers, "cookie") || joinHarCookies(entry.request?.cookies),
      xFbLsd: findHeader(entry.request?.headers, "x-fb-lsd"),
      xAsbdId: findHeader(entry.request?.headers, "x-asbd-id"),
      userAgent: findHeader(entry.request?.headers, "user-agent")
    };
  } catch {
    return null;
  }
}

function decodeJsStringLiteral(literal) {
  try {
    return Function(`"use strict"; return (${literal});`)();
  } catch {
    return "";
  }
}

function decodeShellOrJsLiteral(literal) {
  const text = String(literal ?? "").trim();
  if (!text) {
    return "";
  }
  if ((text.startsWith('"') && text.endsWith('"')) || (text.startsWith("'") && text.endsWith("'"))) {
    const jsDecoded = decodeJsStringLiteral(text);
    if (jsDecoded) {
      return String(jsDecoded);
    }
    return text.slice(1, -1);
  }
  return text;
}

function normalizeWindowsCmdCurl(text) {
  return String(text ?? "")
    .replace(/\^\r?\n/g, " ")
    .replaceAll("^", "");
}

function parseFetchText(text) {
  const bodyMatch = text.match(/\bbody\s*:\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/s);
  if (!bodyMatch) {
    return null;
  }

  const headerValue = (name) => {
    const pattern = new RegExp(`["']${name}["']\\s*:\\s*("(?:[^"\\\\]|\\\\.)*"|'(?:[^'\\\\]|\\\\.)*')`, "is");
    const match = text.match(pattern);
    return decodeShellOrJsLiteral(match?.[1] || "");
  };

  return {
    sourceType: "fetch",
    formTemplate: decodeShellOrJsLiteral(bodyMatch[1]),
    cookie: headerValue("cookie"),
    xFbLsd: headerValue("x-fb-lsd"),
    xAsbdId: headerValue("x-asbd-id"),
    userAgent: headerValue("user-agent")
  };
}

function parseCurlText(text) {
  const normalizedText = normalizeWindowsCmdCurl(text);
  const bodyMatch = normalizedText.match(/(?:--data-raw|--data|-d)\s+("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/s);
  if (!bodyMatch) {
    return null;
  }

  const headers = [];
  const headerPattern = /-H\s+("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/gs;
  for (const match of normalizedText.matchAll(headerPattern)) {
    const decoded = decodeShellOrJsLiteral(match[1]);
    const splitIndex = decoded.indexOf(":");
    if (splitIndex <= 0) {
      continue;
    }
    headers.push({
      name: decoded.slice(0, splitIndex).trim(),
      value: decoded.slice(splitIndex + 1).trim()
    });
  }

  const headerValue = (name) =>
    headers.find((header) => header.name.toLowerCase() === String(name).toLowerCase())?.value || "";

  const cookieMatch = normalizedText.match(/(?:-b|--cookie)\s+("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/s);
  const cookie = decodeShellOrJsLiteral(cookieMatch?.[1] || "");

  return {
    sourceType: "curl",
    formTemplate: decodeShellOrJsLiteral(bodyMatch[1]),
    cookie: cookie || headerValue("cookie"),
    xFbLsd: headerValue("x-fb-lsd"),
    xAsbdId: headerValue("x-asbd-id"),
    userAgent: headerValue("user-agent")
  };
}

function parseAny(text) {
  return tryParseHar(text) || parseFetchText(text) || parseCurlText(text);
}

function printResult(result) {
  if (!result) {
    console.error(
      "Could not extract a Meta session. Provide either a HAR with AdLibrarySearchPaginationQuery, or a saved 'Copy as fetch' / 'Copy as cURL' request."
    );
    process.exit(1);
  }

  console.log(`# Source: ${result.sourceType}`);
  console.log(`META_GRAPHQL_FORM_TEMPLATE=${result.formTemplate}`);
  if (result.cookie) {
    console.log(`META_COOKIE=${result.cookie}`);
  } else {
    console.log("# META_COOKIE was not found in this source.");
  }
  if (result.xFbLsd) {
    console.log(`META_X_FB_LSD=${result.xFbLsd}`);
  }
  if (result.xAsbdId) {
    console.log(`META_X_ASBD_ID=${result.xAsbdId}`);
  }
  if (result.userAgent) {
    console.log(`META_USER_AGENT=${result.userAgent}`);
  }

  if (!result.cookie) {
    console.log(
      "# This source did not include cookies. Use Chrome/Edge DevTools on the same request and 'Copy as fetch' or 'Copy as cURL' to capture META_COOKIE."
    );
  }
}

const inputPath = process.argv[2];
if (!inputPath) {
  console.error("Usage: node scripts/extract-meta-session.js <path-to-har-or-fetch-or-curl.txt>");
  process.exit(1);
}

const text = fs.readFileSync(inputPath, "utf8");
printResult(parseAny(text));
