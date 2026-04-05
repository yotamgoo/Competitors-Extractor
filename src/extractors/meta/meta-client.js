import { randomUUID } from "node:crypto";

import { normalizePageId, nowIso } from "./utils.js";

const DEFAULT_API_URL = "https://www.facebook.com/api/graphql/";
const DEFAULT_DOC_ID = "25987067537594875";
const DEFAULT_FRIENDLY_NAME = "AdLibrarySearchPaginationQuery";
const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";

function normalizeMediaChoice(value) {
  const text = String(value ?? "").trim().toLowerCase();
  if (text === "image" || text === "video" || text === "both") {
    return text;
  }
  return "both";
}

function toPositiveInt(value, fallback, minimum = 1, maximum = 100000) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(minimum, Math.min(maximum, parsed));
}

function buildSearchUrl(searchQuery, pageId) {
  const url = new URL("https://www.facebook.com/ads/library/");
  url.searchParams.set("active_status", "active");
  url.searchParams.set("ad_type", "all");
  url.searchParams.set("country", "US");
  url.searchParams.set("is_targeted_country", "false");
  url.searchParams.set("media_type", "all");
  url.searchParams.set("search_type", "page");
  url.searchParams.set("sort_data[mode]", "total_impressions");
  url.searchParams.set("sort_data[direction]", "desc");
  url.searchParams.set("view_all_page_id", pageId);
  if (searchQuery.trim()) {
    url.searchParams.set("q", searchQuery.trim());
  }
  return url.toString();
}

function parseMetaDate(raw) {
  const clean = String(raw ?? "").replace(/\s+/g, " ").trim();
  if (!clean) {
    return null;
  }

  const direct = new Date(clean);
  if (!Number.isNaN(direct.valueOf())) {
    return direct;
  }

  const parts = clean
    .replace(",", " ")
    .split(/\s+/)
    .map((item) => item.trim())
    .filter(Boolean);
  if (parts.length < 3) {
    return null;
  }

  const monthLookup = {
    jan: 0,
    january: 0,
    feb: 1,
    february: 1,
    mar: 2,
    march: 2,
    apr: 3,
    april: 3,
    may: 4,
    jun: 5,
    june: 5,
    jul: 6,
    july: 6,
    aug: 7,
    august: 7,
    sep: 8,
    sept: 8,
    september: 8,
    oct: 9,
    october: 9,
    nov: 10,
    november: 10,
    dec: 11,
    december: 11
  };

  let day = Number.NaN;
  let month = Number.NaN;
  let year = Number.NaN;

  if (Number.isFinite(Number(parts[0])) && monthLookup[parts[1].toLowerCase()] !== undefined) {
    day = Number(parts[0]);
    month = monthLookup[parts[1].toLowerCase()];
    year = Number(parts[2]);
  } else if (monthLookup[parts[0].toLowerCase()] !== undefined && Number.isFinite(Number(parts[1]))) {
    month = monthLookup[parts[0].toLowerCase()];
    day = Number(parts[1]);
    year = Number(parts[2]);
  }

  if (!Number.isFinite(day) || !Number.isFinite(month) || !Number.isFinite(year)) {
    return null;
  }

  return new Date(Date.UTC(year, month, day, 0, 0, 0));
}

function daysBetween(today, startedDate) {
  const todayUtc = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate());
  const startUtc = Date.UTC(startedDate.getUTCFullYear(), startedDate.getUTCMonth(), startedDate.getUTCDate());
  return Math.max(Math.floor((todayUtc - startUtc) / 86400000), 0);
}

function firstText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) {
      return text;
    }
  }
  return "";
}

function looksLikeTemplateText(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return false;
  }
  return /\{\{[^}]+\}\}/.test(text);
}

function humanizeCta(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "";
  }
  if (raw.includes("_")) {
    return raw
      .toLowerCase()
      .split("_")
      .filter(Boolean)
      .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
      .join(" ");
  }
  return raw;
}

function unixToIsoDate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return new Date(Math.trunc(numeric) * 1000).toISOString().slice(0, 10);
}

function pickGraphMedia(snapshot) {
  const cards = Array.isArray(snapshot.cards) ? snapshot.cards : [];
  for (const card of cards) {
    if (!card || typeof card !== "object") {
      continue;
    }
    const video = firstText(
      card.video_hd_url,
      card.video_sd_url,
      card.watermarked_video_hd_url,
      card.watermarked_video_sd_url,
      card.video_url
    );
    if (video) {
      return { mediaType: "video", mediaUrl: video };
    }
  }

  const videos = Array.isArray(snapshot.videos) ? snapshot.videos : [];
  for (const videoItem of videos) {
    if (!videoItem || typeof videoItem !== "object") {
      continue;
    }
    const video = firstText(
      videoItem.video_hd_url,
      videoItem.video_sd_url,
      videoItem.watermarked_video_hd_url,
      videoItem.watermarked_video_sd_url,
      videoItem.url,
      videoItem.video_url
    );
    if (video) {
      return { mediaType: "video", mediaUrl: video };
    }
  }

  const directVideo = firstText(
    snapshot.video_hd_url,
    snapshot.video_sd_url,
    snapshot.watermarked_video_hd_url,
    snapshot.watermarked_video_sd_url
  );
  if (directVideo) {
    return { mediaType: "video", mediaUrl: directVideo };
  }

  for (const card of cards) {
    if (!card || typeof card !== "object") {
      continue;
    }
    const image = firstText(
      card.original_image_url,
      card.resized_image_url,
      card.watermarked_resized_image_url,
      card.image_url
    );
    if (image) {
      return { mediaType: "image", mediaUrl: image };
    }
  }

  const images = Array.isArray(snapshot.images) ? snapshot.images : [];
  for (const imageItem of images) {
    if (!imageItem || typeof imageItem !== "object") {
      continue;
    }
    const image = firstText(
      imageItem.original_image_url,
      imageItem.resized_image_url,
      imageItem.watermarked_resized_image_url,
      imageItem.image_url,
      imageItem.url
    );
    if (image) {
      return { mediaType: "image", mediaUrl: image };
    }
  }

  const fallbackImage = firstText(snapshot.video_preview_image_url, snapshot.original_image_url, snapshot.resized_image_url);
  if (fallbackImage) {
    return { mediaType: "image", mediaUrl: fallbackImage };
  }

  return { mediaType: "unknown", mediaUrl: "" };
}

function inferMediaTypeFromUrl(mediaUrl) {
  const value = String(mediaUrl ?? "").trim().toLowerCase();
  if (!value) {
    return "unknown";
  }
  if (value.includes(".mp4") || value.includes(".webm") || value.includes(".m3u8") || value.includes("video")) {
    return "video";
  }
  if (
    value.includes(".jpg") ||
    value.includes(".jpeg") ||
    value.includes(".png") ||
    value.includes(".webp") ||
    value.includes(".gif") ||
    value.includes("image")
  ) {
    return "image";
  }
  return "unknown";
}

function toTitleCaseTokens(values) {
  return values
    .map((item) => item.trim().toLowerCase().replaceAll("_", " "))
    .filter(Boolean)
    .map((item) => item.replace(/\b\w/g, (match) => match.toUpperCase()))
    .join(", ");
}

function extractTextField(value) {
  if (typeof value === "string") {
    return value;
  }
  if (!value || typeof value !== "object") {
    return "";
  }
  return firstText(value.text, value.body_text, value.message, value.content);
}

function firstResolvedText(...values) {
  let templateFallback = "";
  for (const value of values) {
    const text = extractTextField(value);
    if (!text) {
      continue;
    }
    if (!looksLikeTemplateText(text)) {
      return text;
    }
    if (!templateFallback) {
      templateFallback = text;
    }
  }
  return templateFallback;
}

function toRawCandidate(row) {
  const snapshot = row.snapshot ?? row.ad_snapshot ?? row.rendering_snapshot ?? {};
  const cards = Array.isArray(snapshot.cards) ? snapshot.cards : [];
  const firstCard = cards[0] ?? {};
  const branded = snapshot.branded_content ?? {};
  const media = pickGraphMedia(snapshot);
  const fallbackMediaUrl = firstText(
    row.media_url,
    row.video_url,
    row.image_url,
    row.video_hd_url,
    row.video_sd_url,
    row.original_image_url,
    row.resized_image_url
  );
  const mediaUrl = firstText(media.mediaUrl, fallbackMediaUrl);
  const mediaType = media.mediaType === "unknown" ? inferMediaTypeFromUrl(mediaUrl) : media.mediaType;

  const platformListRaw = firstText(
    Array.isArray(row.publisher_platform) ? row.publisher_platform.join(",") : "",
    Array.isArray(row.publisher_platforms) ? row.publisher_platforms.join(",") : "",
    Array.isArray(row.platforms) ? row.platforms.join(",") : "",
    String(row.platforms ?? "")
  );
  const platformList = platformListRaw
    .split(/[,\|;]/)
    .map((item) => item.trim())
    .filter(Boolean);

  const categories = Array.isArray(snapshot.page_categories)
    ? snapshot.page_categories.map((item) => String(item ?? "").trim()).filter(Boolean).join(", ")
    : firstText(
        Array.isArray(row.categories) ? row.categories.join(", ") : "",
        String(row.category ?? "")
      );

  const libraryId = firstText(row.ad_archive_id, row.ad_id, row.id).replace(/[^\d]/g, "");
  if (!libraryId) {
    return null;
  }

  const startedRunningText = firstText(
    unixToIsoDate(row.start_date),
    unixToIsoDate(row.start_date_utc),
    unixToIsoDate(row.startDate),
    String(row.started_running_date ?? ""),
    String(row.started_running_text ?? "")
  );

  const runningDaysHint = Number(row.running_days);

  return {
    libraryId,
    advertiser: firstText(
      branded.page_name,
      snapshot.page_name,
      row.page_name,
      row.pageName,
      row.advertiser_name,
      row.advertiser
    ),
    startedRunningText,
    runningDaysHint: Number.isFinite(runningDaysHint) ? runningDaysHint : null,
    adCopy: firstResolvedText(
      firstCard.body,
      snapshot.body,
      row.ad_copy,
      row.body,
      row.description
    ),
    headline: firstResolvedText(firstCard.title, snapshot.title, row.title, row.headline),
    cta: firstText(
      snapshot.cta_text,
      firstCard.cta_text,
      row.cta_text,
      row.cta,
      humanizeCta(firstText(snapshot.cta_type, firstCard.cta_type, row.cta_type))
    ),
    mediaType,
    mediaUrl,
    landingUrl: firstText(
      snapshot.link_url,
      firstCard.link_url,
      row.link_url,
      row.landing_url,
      row.destination_url,
      row.url
    ),
    platforms: toTitleCaseTokens(platformList),
    categories
  };
}

function collectRows(value, sink, seen, depth = 0) {
  if (!value || typeof value !== "object" || seen.has(value) || depth > 12) {
    return;
  }
  seen.add(value);

  if (Array.isArray(value)) {
    for (const item of value) {
      collectRows(item, sink, seen, depth + 1);
    }
    return;
  }

  if (
    value.ad_archive_id ||
    value.ad_id ||
    value.start_date ||
    value.snapshot ||
    value.ad_snapshot ||
    value.rendering_snapshot
  ) {
    sink.push(value);
  }

  for (const nested of Object.values(value)) {
    collectRows(nested, sink, seen, depth + 1);
  }
}

function extractGraphCandidates(payload) {
  const rows = [];
  collectRows(payload, rows, new Set());
  const byId = new Map();
  for (const row of rows) {
    const candidate = toRawCandidate(row);
    if (!candidate) {
      continue;
    }
    if (!byId.has(candidate.libraryId)) {
      byId.set(candidate.libraryId, candidate);
    }
  }
  return [...byId.values()];
}

function extractPageInfo(value, seen = new Set(), depth = 0) {
  if (!value || typeof value !== "object" || seen.has(value) || depth > 12) {
    return null;
  }
  seen.add(value);

  if (value.page_info && typeof value.page_info === "object") {
    return value.page_info;
  }
  if ("end_cursor" in value || "has_next_page" in value) {
    return value;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = extractPageInfo(item, seen, depth + 1);
      if (found) {
        return found;
      }
    }
    return null;
  }

  for (const nested of Object.values(value)) {
    const found = extractPageInfo(nested, seen, depth + 1);
    if (found) {
      return found;
    }
  }
  return null;
}

function parseJsonSafe(value) {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(String(value));
  } catch {
    return null;
  }
}

function extractTemplateVariables(apiConfig) {
  const templateText = String(apiConfig?.formTemplate ?? "").trim();
  if (!templateText) {
    throw new Error(
      "Missing META_GRAPHQL_FORM_TEMPLATE. Paste one full AdLibrarySearchPaginationQuery form body from HAR into AI Studio secrets."
    );
  }

  const params = new URLSearchParams(templateText);
  const templateVariables = parseJsonSafe(params.get("variables")) || {};
  return { params, templateVariables };
}

function templateCursorValue(templateVariables) {
  return firstText(templateVariables?.cursor);
}

function buildMetaVariables({ pageId, searchQuery, cursor, pageSize, templateVariables, useTemplateCursor }) {
  const template = templateVariables && typeof templateVariables === "object" ? templateVariables : {};
  const resolvedCursor =
    cursor && cursor !== "start"
      ? cursor
      : useTemplateCursor
        ? templateCursorValue(template)
        : null;
  return {
    activeStatus: template.activeStatus ?? "active",
    adType: template.adType ?? "ALL",
    bylines: Array.isArray(template.bylines) ? template.bylines : [],
    collationToken: template.collationToken ?? null,
    contentLanguages: Array.isArray(template.contentLanguages) ? template.contentLanguages : [],
    countries: Array.isArray(template.countries) && template.countries.length ? template.countries : ["US"],
    cursor: resolvedCursor || null,
    excludedIDs: template.excludedIDs ?? null,
    first: pageSize,
    isTargetedCountry: template.isTargetedCountry ?? false,
    location: template.location ?? null,
    mediaType: template.mediaType ?? "all",
    multiCountryFilterMode: template.multiCountryFilterMode ?? null,
    pageIDs: Array.isArray(template.pageIDs) ? template.pageIDs : [],
    potentialReachInput: template.potentialReachInput ?? null,
    publisherPlatforms: Array.isArray(template.publisherPlatforms) ? template.publisherPlatforms : [],
    queryString: searchQuery,
    regions: template.regions ?? null,
    searchType: template.searchType ?? "page",
    sessionID: firstText(template.sessionID, template.sessionId, randomUUID()),
    sortData:
      template.sortData && typeof template.sortData === "object"
        ? template.sortData
        : { direction: "DESCENDING", mode: "SORT_BY_TOTAL_IMPRESSIONS" },
    source: template.source ?? null,
    startDate: template.startDate ?? null,
    v: firstText(template.v, "cb473e"),
    viewAllPageID: pageId
  };
}

function prepareRequestParams(apiConfig, { pageId, searchQuery, cursor, pageSize, useTemplateCursor = false }) {
  const { params, templateVariables } = extractTemplateVariables(apiConfig);
  const user = firstText(params.get("__user"), params.get("av"), apiConfig?.userId, "0");
  const fbDtsg = firstText(params.get("fb_dtsg"), apiConfig?.fbDtsg);
  const lsd = firstText(params.get("lsd"), apiConfig?.lsd);

  if (user) {
    params.set("__user", user);
    params.set("av", firstText(params.get("av"), user));
  }
  if (fbDtsg) {
    params.set("fb_dtsg", fbDtsg);
  }
  if (lsd) {
    params.set("lsd", lsd);
  }
  params.set("fb_api_caller_class", firstText(params.get("fb_api_caller_class"), "RelayModern"));
  params.set("fb_api_req_friendly_name", firstText(apiConfig?.friendlyName, params.get("fb_api_req_friendly_name"), DEFAULT_FRIENDLY_NAME));
  params.set("server_timestamps", firstText(params.get("server_timestamps"), "true"));
  params.set("doc_id", firstText(apiConfig?.docId, params.get("doc_id"), DEFAULT_DOC_ID));
  params.set(
    "variables",
    JSON.stringify(
      buildMetaVariables({
        pageId,
        searchQuery,
        cursor,
        pageSize,
        templateVariables,
        useTemplateCursor
      })
    )
  );

  return params;
}

function buildMetaHeaders(apiConfig, { pageId, searchQuery, params }) {
  const headers = {
    accept: "*/*",
    "accept-language": firstText(apiConfig?.acceptLanguage, "en-GB,en-US;q=0.9,en;q=0.8,he;q=0.7"),
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded",
    dnt: "1",
    origin: "https://www.facebook.com",
    pragma: "no-cache",
    priority: "u=1, i",
    referer: buildSearchUrl(searchQuery, pageId),
    "sec-ch-prefers-color-scheme": "light",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-full-version-list":
      '"Chromium";v="146.0.7680.165", "Not-A.Brand";v="24.0.0.0", "Google Chrome";v="146.0.7680.165"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": '""',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-platform-version": '"19.0.0"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": firstText(apiConfig?.userAgent, DEFAULT_USER_AGENT),
    "x-fb-friendly-name": firstText(apiConfig?.friendlyName, DEFAULT_FRIENDLY_NAME)
  };

  const xAsbdId = firstText(apiConfig?.xAsbdId);
  if (xAsbdId) {
    headers["x-asbd-id"] = xAsbdId;
  }

  const lsd = firstText(apiConfig?.lsd, params.get("lsd"));
  if (lsd) {
    headers["x-fb-lsd"] = lsd;
  }

  const cookie = firstText(apiConfig?.cookie);
  if (cookie) {
    headers.cookie = cookie;
  }

  return headers;
}

async function fetchMetaPayload(apiConfig, requestOptions) {
  const params = prepareRequestParams(apiConfig, requestOptions);
  const headers = buildMetaHeaders(apiConfig, { ...requestOptions, params });
  const apiUrl = firstText(apiConfig?.url, DEFAULT_API_URL);
  const timeoutMs = toPositiveInt(apiConfig?.timeoutMs, 90000, 1000, 300000);

  let response;
  try {
    response = await fetch(apiUrl, {
      method: "POST",
      headers,
      body: params.toString(),
      signal: AbortSignal.timeout(timeoutMs)
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Meta GraphQL request failed before a response was returned. ${message}`);
  }

  const rawText = await response.text().catch(() => "");
  return parseMetaPayloadFromText(rawText, {
    status: response.status,
    ok: response.ok
  });
}

function parseMetaPayloadFromText(rawText, { status, ok }) {
  const normalized = String(rawText ?? "").startsWith("for (;;);") ? String(rawText).slice(9) : String(rawText ?? "");
  if (!ok) {
    const snippet = normalized.replace(/\s+/g, " ").trim().slice(0, 260);
    throw new Error(
      `Meta GraphQL request failed (${status}). Refresh META_GRAPHQL_FORM_TEMPLATE and Meta session values. ${snippet}`
    );
  }

  let payload;
  try {
    payload = JSON.parse(normalized);
  } catch {
    const snippet = normalized.replace(/\s+/g, " ").trim().slice(0, 260);
    throw new Error(
      `Meta GraphQL returned non-JSON output. Refresh META_GRAPHQL_FORM_TEMPLATE and confirm the session is still valid. ${snippet}`
    );
  }

  if (payload && typeof payload === "object" && (payload.error || payload.errorSummary || payload.errorDescription)) {
    const summary = firstText(payload.errorSummary, payload.errorDescription);
    throw new Error(
      `Meta session/login request was rejected. Refresh META_GRAPHQL_FORM_TEMPLATE from a fresh logged-in HAR. ${summary}`
    );
  }

  const firstError = Array.isArray(payload?.errors) ? payload.errors[0] : null;
  if (firstError) {
    const message = firstText(firstError.message, firstError.summary, JSON.stringify(firstError).slice(0, 260));
    throw new Error(`Meta GraphQL returned an error response. ${message}`);
  }

  return payload;
}

function normalizeCandidate(candidate, { today, minDays, media, scrapedAt }) {
  const libraryId = String(candidate.libraryId ?? "").trim();
  if (!libraryId) {
    return null;
  }

  const startedDate = parseMetaDate(candidate.startedRunningText);
  const runningDays =
    startedDate ??
    (typeof candidate.runningDaysHint === "number" && Number.isFinite(candidate.runningDaysHint)
      ? candidate.runningDaysHint
      : null);
  const resolvedRunningDaysValue =
    runningDays instanceof Date ? daysBetween(today, runningDays) : runningDays ?? null;
  if (!Number.isFinite(resolvedRunningDaysValue)) {
    return null;
  }
  const resolvedRunningDays = Number(resolvedRunningDaysValue);
  if (resolvedRunningDays < minDays) {
    return null;
  }

  const mediaUrl = String(candidate.mediaUrl ?? "").trim();
  if (!mediaUrl) {
    return null;
  }

  const inferredMediaType =
    candidate.mediaType === "unknown" ? inferMediaTypeFromUrl(mediaUrl) : String(candidate.mediaType ?? "").trim();
  if (inferredMediaType !== "image" && inferredMediaType !== "video") {
    return null;
  }
  if (media !== "both" && inferredMediaType !== media) {
    return null;
  }

  const advertiser = String(candidate.advertiser ?? "").trim();
  const headline = String(candidate.headline ?? "").trim();
  const adCopy = String(candidate.adCopy ?? "").trim();
  const landingUrl = String(candidate.landingUrl ?? "").trim();
  if (!advertiser && !headline && !adCopy && !landingUrl) {
    return null;
  }

  const startedRunningDate = startedDate
    ? startedDate.toISOString().slice(0, 10)
    : new Date(Date.now() - resolvedRunningDays * 86400000).toISOString().slice(0, 10);

  return {
    libraryId,
    advertiser,
    startedRunningDate,
    runningDays: resolvedRunningDays,
    adCopy,
    headline,
    cta: String(candidate.cta ?? "").trim(),
    mediaType: inferredMediaType,
    mediaUrl,
    adLink: `https://www.facebook.com/ads/library/?id=${libraryId}`,
    landingUrl,
    platforms: String(candidate.platforms ?? "").trim(),
    categories: String(candidate.categories ?? "").trim(),
    scrapedAt
  };
}

export async function scrapeMetaAdsSlice(options) {
  const log = options.log ?? console.log;
  const pageId = normalizePageId(options.pageId);
  if (!pageId) {
    throw new Error("Meta page scans require a numeric page ID.");
  }

  const searchQuery = String(options.searchQuery ?? "").trim();
  const minDays = toPositiveInt(options.minDays, 0, 0, 3650);
  const media = normalizeMediaChoice(options.media);
  const startPosition = toPositiveInt(options.startPosition, 0, 0, 100000);
  const maxAdsPerPage = toPositiveInt(options.maxAdsPerPage, 100, 1, 1000);
  const sliceMaxAds = toPositiveInt(options.sliceMaxAds, 30, 1, 100);
  const cursor = String(options.cursor ?? "").trim() || "start";
  const inputLastLibraryId = String(options.lastLibraryId ?? "").trim();
  const templateCursor = (() => {
    try {
      return templateCursorValue(extractTemplateVariables(options.apiConfig).templateVariables);
    } catch {
      return "";
    }
  })();

  if (startPosition >= maxAdsPerPage) {
    return {
      pageId,
      pageName: "",
      ads: [],
      nextCursor: "",
      nextPosition: startPosition,
      lastLibraryId: inputLastLibraryId,
      reachedEnd: true,
      breakReason: "page-limit",
      graphResponseCount: 0,
      graphCandidateCount: 0,
      diagnostics: ""
    };
  }

  let usingTemplateCursor = false;
  log(`Requesting Meta GraphQL page for ${pageId} (${cursor === "start" ? "start" : "cursor"})...`);
  let payload = await fetchMetaPayload(options.apiConfig, {
    pageId,
    searchQuery,
    cursor,
    pageSize: sliceMaxAds,
    apiConfig: options.apiConfig,
    log
  });

  let candidates = extractGraphCandidates(payload);
  let pageInfo = extractPageInfo(payload);
  if (!candidates.length && !pageInfo && cursor === "start" && templateCursor) {
    usingTemplateCursor = true;
    log("Blank start cursor returned no payload. Retrying with the HAR template cursor.");
    payload = await fetchMetaPayload(options.apiConfig, {
      pageId,
      searchQuery,
      cursor,
      pageSize: sliceMaxAds,
      useTemplateCursor: true,
      apiConfig: options.apiConfig,
      log
    });
    candidates = extractGraphCandidates(payload);
    pageInfo = extractPageInfo(payload);
  }

  const nextCursor = firstText(pageInfo?.end_cursor);
  const hasNextPage = Boolean(pageInfo?.has_next_page && nextCursor);
  if (!candidates.length && !pageInfo) {
    throw new Error(
      "Meta GraphQL response did not contain ad rows or page_info. Refresh META_GRAPHQL_FORM_TEMPLATE from a fresh HAR/session."
    );
  }

  const scrapedAt = nowIso();
  const today = new Date();
  const storedRecords = [];
  let pageName = "";
  let lastLibraryId = inputLastLibraryId;
  let breakReason = hasNextPage ? "cursor-advanced" : "no-next-page";

  for (const candidate of candidates) {
    const candidateId = String(candidate.libraryId ?? "").trim();
    if (candidateId) {
      lastLibraryId = candidateId;
    }

    const normalized = normalizeCandidate(candidate, {
      today,
      minDays,
      media,
      scrapedAt
    });
    if (!normalized) {
      continue;
    }

    if (!pageName && normalized.advertiser) {
      pageName = normalized.advertiser;
    }

    if (startPosition + storedRecords.length >= maxAdsPerPage) {
      breakReason = "page-limit";
      break;
    }

    storedRecords.push(normalized);
    log(
      `Collected [${startPosition + storedRecords.length}/${maxAdsPerPage}] ` +
        `${normalized.advertiser || "Unknown"} | ${normalized.libraryId} | ` +
        `${normalized.mediaType} | ${normalized.runningDays} days`
    );
  }

  const nextPosition = startPosition + storedRecords.length;
  return {
    pageId,
    pageName,
    ads: storedRecords,
    nextCursor: breakReason === "page-limit" ? "" : nextCursor,
    nextPosition,
    lastLibraryId,
    reachedEnd: breakReason === "page-limit" || !hasNextPage,
    breakReason,
    graphResponseCount: 1,
    graphCandidateCount: candidates.length,
    diagnostics: `${usingTemplateCursor ? "used_template_cursor=true;" : ""}${hasNextPage ? `next_cursor=${nextCursor}` : ""}`
  };
}
