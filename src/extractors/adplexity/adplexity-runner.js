import { ensureAdplexityConfigured } from "./config.js";
import { AdplexityClient, PAGE_SIZE } from "./adplexity-client.js";
import {
  cleanHtmlText,
  coerceIntList,
  displayBrandFromUrl,
  formatPlatformLabel,
  normalizeCountryList,
  normalizeDateTime,
  normalizeDaysRunning,
  normalizePlatformList,
  parseJsonObject,
  pickFirstText,
  serializeJson,
  uniqueNumbers
} from "./utils.js";

function buildPageCheckpointKey(reportId, offset) {
  return `adplexity|report:${reportId}|page:${offset}`;
}

function buildDetailCheckpointKey(reportId, adId) {
  return `adplexity|report:${reportId}|ad:${adId}`;
}

function toAdplexityId(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? Math.trunc(numeric) : null;
}

function buildOutputAdId(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (/^\d+$/.test(text)) {
      return `adplexity:${text}`;
    }
  }
  return "";
}

function isAdplexityTrackerUrl(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return false;
  }
  return /(?:app|meta)\.adplexity\.(?:io|com)\/ad\//i.test(text);
}

function normalizePossibleLandingPageUrl(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) {
      continue;
    }
    if (isAdplexityTrackerUrl(text)) {
      continue;
    }
    if (/\.(?:jpg|jpeg|png|gif|webp|svg|mp4|mov|avi|m3u8)(?:[?#].*)?$/i.test(text)) {
      continue;
    }
    if (/^https?:\/\//i.test(text)) {
      return text;
    }
    if (!text.includes(" ") && text.includes(".") && !text.startsWith("{") && !text.startsWith("[")) {
      return `https://${text}`;
    }
  }
  return "";
}

function collectNestedUrlValues(value, output, depth = 0) {
  if (value === undefined || value === null || value === "" || depth > 4) {
    return;
  }

  if (typeof value === "string" || typeof value === "number") {
    output.push(value);
    return;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      collectNestedUrlValues(item, output, depth + 1);
    }
    return;
  }

  if (typeof value === "object") {
    for (const [key, nestedValue] of Object.entries(value)) {
      const normalizedKey = String(key ?? "").trim();
      if (normalizedKey) {
        output.push(normalizedKey);
      }
      collectNestedUrlValues(nestedValue, output, depth + 1);
    }
  }
}

function collectLandingPageCandidates(source, output, depth = 0) {
  if (!source || typeof source !== "object" || depth > 2) {
    return;
  }

  for (const [rawKey, value] of Object.entries(source)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }

    const key = String(rawKey).trim().toLowerCase();
    if (!key) {
      continue;
    }

    if (typeof value === "string" || typeof value === "number") {
      if (/(thumb|image|video|creative|preview|avatar|icon|logo)/i.test(key)) {
        continue;
      }
      if (
        key === "landing_page_url" ||
        key === "landingpageurl" ||
        key === "link_url" ||
        key === "linkurl" ||
        key === "final_url" ||
        key === "finalurl" ||
        key === "redirect_url" ||
        key === "redirecturl" ||
        key === "lp_url" ||
        key === "lpurl" ||
        key === "offer_url" ||
        key === "offerurl" ||
        key === "destination_url" ||
        key === "destinationurl" ||
        key === "target_url" ||
        key === "targeturl" ||
        key === "url" ||
        key === "href" ||
        key === "link" ||
        key === "domain" ||
        key === "host"
      ) {
        output.push(value);
      }
      continue;
    }

    if (typeof value === "object") {
      if (
        key === "meta" ||
        key === "data" ||
        key === "attributes" ||
        key === "links" ||
        key === "destination" ||
        key === "target" ||
        key === "offer"
      ) {
        collectLandingPageCandidates(value, output, depth + 1);
      }
    }
  }
}

function pickLandingPageUrl(...records) {
  const candidates = [];

  for (const record of records) {
    if (!record || typeof record !== "object") {
      continue;
    }

    candidates.push(
      record.landing_page_url,
      record.landingPageUrl,
      record.link_url,
      record.linkUrl,
      record.final_url,
      record.finalUrl,
      record.redirect_url,
      record.redirectUrl,
      record.lp_url,
      record.lpUrl,
      record.lp_redirects,
      record.lpRedirects,
      record.offer_url,
      record.offerUrl,
      record.destination_url,
      record.destinationUrl,
      record.target_url,
      record.targetUrl,
      record.url,
      record.href,
      record.link,
      record.domain,
      record.host
    );

    collectNestedUrlValues(record.lp_url, candidates);
    collectNestedUrlValues(record.lpUrl, candidates);
    collectNestedUrlValues(record.lp_redirects, candidates);
    collectNestedUrlValues(record.lpRedirects, candidates);

    collectLandingPageCandidates(record, candidates);
  }

  return normalizePossibleLandingPageUrl(...candidates);
}

function pickTrackerUrl(...records) {
  const candidates = [];

  for (const record of records) {
    if (!record || typeof record !== "object") {
      continue;
    }

    candidates.push(
      record.url,
      record.href,
      record.link,
      record.link_url,
      record.linkUrl,
      record.redirect_url,
      record.redirectUrl,
      record.final_url,
      record.finalUrl,
      record.destination_url,
      record.destinationUrl,
      record.target_url,
      record.targetUrl
    );

    collectLandingPageCandidates(record, candidates);
  }

  for (const candidate of candidates) {
    const text = String(candidate ?? "").trim();
    if (isAdplexityTrackerUrl(text)) {
      return text;
    }
  }

  return "";
}

function collectUrlLikeEntries(source, output, prefix = "", depth = 0) {
  if (!source || typeof source !== "object" || depth > 2) {
    return;
  }

  for (const [rawKey, value] of Object.entries(source)) {
    const key = String(rawKey ?? "").trim();
    if (!key) {
      continue;
    }

    const path = prefix ? `${prefix}.${key}` : key;

    if (typeof value === "string" || typeof value === "number") {
      const text = String(value ?? "").trim();
      if (!text) {
        continue;
      }
      if (
        /(url|link|href|domain|host|landing|target|destination|offer)/i.test(key) ||
        /^https?:\/\//i.test(text)
      ) {
        output.push(`${path}=${text}`);
      }
      continue;
    }

    if (value && typeof value === "object" && !Array.isArray(value)) {
      if (/^(lp_url|lpUrl|lp_redirects|lpRedirects)$/i.test(key)) {
        const nestedCandidates = [];
        collectNestedUrlValues(value, nestedCandidates);
        for (const candidate of nestedCandidates) {
          const text = String(candidate ?? "").trim();
          if (text) {
            output.push(`${path}=>${text}`);
          }
        }
      }
      collectUrlLikeEntries(value, output, path, depth + 1);
    }
  }
}

function shouldDebugLandingPage(url) {
  const text = String(url ?? "").trim();
  if (!text) {
    return true;
  }
  return isAdplexityTrackerUrl(text);
}

function inferBrandFromPageName(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (!text) {
      continue;
    }
    if (/^\d+$/.test(text)) {
      continue;
    }
    if (text.split(/\s+/).length <= 8) {
      return text;
    }
  }
  return "";
}

function stagePriority(stage) {
  if (stage === "ad-detail") {
    return 0;
  }
  if (stage === "report-page") {
    return 1;
  }
  return 9;
}

function selectRunnableCheckpoints(checkpoints, retryLimit, staleMinutes) {
  const staleBefore = Date.now() - staleMinutes * 60 * 1000;

  return checkpoints
    .filter((checkpoint) => {
      if (checkpoint.status === "pending") {
        return true;
      }
      if (checkpoint.status === "failed") {
        return checkpoint.attemptCount < retryLimit;
      }
      if (checkpoint.status === "running") {
        const heartbeatMs = checkpoint.heartbeatAt ? Date.parse(checkpoint.heartbeatAt) : 0;
        return Boolean(heartbeatMs && heartbeatMs < staleBefore);
      }
      return false;
    })
    .sort((left, right) => {
      const reportDelta = (left.reportId || 0) - (right.reportId || 0);
      if (reportDelta) {
        return reportDelta;
      }
      const stageDelta = stagePriority(left.stage) - stagePriority(right.stage);
      if (stageDelta) {
        return stageDelta;
      }
      const leftCursor = Number(left.cursor || 0);
      const rightCursor = Number(right.cursor || 0);
      if (Number.isFinite(leftCursor) && Number.isFinite(rightCursor) && leftCursor !== rightCursor) {
        return leftCursor - rightCursor;
      }
      const leftAdId = Number(left.adId || 0);
      const rightAdId = Number(right.adId || 0);
      if (Number.isFinite(leftAdId) && Number.isFinite(rightAdId) && leftAdId !== rightAdId) {
        return leftAdId - rightAdId;
      }
      return left.key.localeCompare(right.key);
    });
}

function inferBrandFromTitle(title) {
  const text = String(title ?? "").trim();
  if (!text) {
    return "";
  }

  if (text.includes(":")) {
    const candidate = text.split(":").at(-1)?.trim() ?? "";
    if (candidate && candidate.split(/\s+/).length <= 5) {
      return candidate;
    }
  }

  if (text.includes("|")) {
    const candidate = text.split("|")[0]?.trim() ?? "";
    if (candidate && candidate.split(/\s+/).length <= 5) {
      return candidate;
    }
  }

  return "";
}

function formatDateOnly(value) {
  const normalized = normalizeDateTime(value);
  return normalized ? normalized.slice(0, 10) : "";
}

function normalizeListingAd(listing) {
  const adplexityId = toAdplexityId(listing.id);
  if (adplexityId === null) {
    throw new Error("AdPlexity listing is missing a numeric id.");
  }

  const title = String(listing.title ?? listing.title_en ?? "").trim();
  const landingPage = pickLandingPageUrl(listing);
  const countries = normalizeCountryList(listing.countries);
  const imageUrl = String(listing.thumb_url ?? "").trim() || "";
  const outputAdId = buildOutputAdId(listing.ad_id, listing.meta_ad_id, adplexityId);
  const legacyAdId = `adplexity:${adplexityId}`;
  const trackerUrl = pickTrackerUrl(listing);
  const brand = pickFirstText(
    listing.advertiser,
    listing.advertiser_name,
    listing.brand,
    listing.page_name,
    listing.page_title,
    inferBrandFromPageName(listing.page_name, listing.page_title, listing.pageName),
    inferBrandFromTitle(title),
    displayBrandFromUrl(landingPage)
  );

  return {
    adId: outputAdId,
    legacyAdIds: outputAdId && outputAdId !== legacyAdId ? [legacyAdId] : [],
    adCopy: "",
    adUrl: `https://app.adplexity.io/ad/${adplexityId}`,
    brand,
    categories: [],
    country: countries.join(", "),
    cta: "",
    daysRunning: normalizeDaysRunning(listing.days_total ?? listing.hits_total, {
      firstSeen: listing.first_seen,
      lastSeen: listing.last_seen
    }),
    firstSeen: formatDateOnly(listing.first_seen),
    landingPageUrl: landingPage,
    lastSeen: formatDateOnly(listing.last_seen),
    mediaUrl: imageUrl,
    platforms: [],
    productCategory: "",
    status: Number(listing.meta_status ?? 0) === 1 ? "active" : "inactive",
    trackerUrl,
    title,
    winner: false
  };
}

function normalizeDetailedAd(adplexityId, listing, detail) {
  const adData = detail?.ad && typeof detail.ad === "object" ? detail.ad : {};
  const meta = adData?.meta && typeof adData.meta === "object" ? adData.meta : {};
  const listingData = listing && typeof listing === "object" ? listing : {};
  const videos = Array.isArray(detail?.videos)
    ? detail.videos
    : Array.isArray(meta.videos)
      ? meta.videos
      : [];
  const videoUrl = String(videos?.[0]?.url ?? "").trim() || "";
  const imageUrl = String(listingData.thumb_url ?? "").trim() || "";
  const title = pickFirstText(listingData.title, listingData.title_en, adData.title);
  const landingPageUrl = pickLandingPageUrl(listingData, meta, adData, detail);
  const brand = pickFirstText(
    adData.advertiser,
    adData.advertiser_name,
    meta.advertiser,
    meta.advertiser_name,
    meta.brand,
    meta.page_name,
    meta.page_title,
    meta.pageName,
    meta.pageTitle,
    adData.page_name,
    adData.page_title,
    listingData.page_name,
    listingData.page_title,
    listingData.advertiser,
    listingData.advertiser_name,
    listingData.brand,
    inferBrandFromPageName(
      meta.page_name,
      meta.page_title,
      meta.pageName,
      meta.pageTitle,
      adData.page_name,
      adData.page_title,
      listingData.page_name,
      listingData.page_title
    ),
    inferBrandFromTitle(title),
    displayBrandFromUrl(landingPageUrl)
  );
  const countries = normalizeCountryList(listingData.countries);
  const platforms = normalizePlatformList(meta.platforms).map(formatPlatformLabel);
  const outputAdId = buildOutputAdId(
    meta.ad_id,
    adData.ad_id,
    listingData.ad_id,
    listingData.meta_ad_id,
    adplexityId
  );
  const legacyAdId = `adplexity:${adplexityId}`;
  const trackerUrl = pickTrackerUrl(listingData, meta, adData, detail);

  return {
    adId: outputAdId,
    legacyAdIds: outputAdId && outputAdId !== legacyAdId ? [legacyAdId] : [],
    adCopy: cleanHtmlText(adData.description ?? adData.description_en ?? ""),
    adUrl: `https://app.adplexity.io/ad/${adplexityId}`,
    brand,
    categories: [],
    country: countries.join(", "),
    cta: String(meta.cta_type_name ?? meta.cta_type ?? "").trim(),
    daysRunning: normalizeDaysRunning(listingData.days_total ?? listingData.hits_total, {
      firstSeen: listingData.first_seen,
      lastSeen: listingData.last_seen
    }),
    firstSeen: formatDateOnly(listingData.first_seen),
    landingPageUrl,
    lastSeen: formatDateOnly(listingData.last_seen),
    mediaUrl: videoUrl || imageUrl,
    platforms,
    productCategory: "",
    status: Number(listingData.meta_status ?? 0) === 1 ? "active" : "inactive",
    trackerUrl,
    title,
    winner: false
  };
}

function buildInitialReportCheckpoints(reportIds, reportNameLookup) {
  return reportIds.map((reportId) => ({
    key: buildPageCheckpointKey(reportId, 0),
    stage: "report-page",
    reportId,
    reportName: reportNameLookup.get(reportId) ?? String(reportId),
    cursor: "0",
    adId: "",
    snapshotJson: ""
  }));
}

export async function seedAdplexityQueue({ airtable, config, runtime, payload }) {
  ensureAdplexityConfigured();
  runtime.begin("seed-queue");
  const log = runtime.log;

  try {
    const manualReportIds = coerceIntList(payload?.manualReportIds);
    const competitorReportIds = manualReportIds.length ? [] : await airtable.getCompetitorReportIds();
    const reportIds = manualReportIds.length ? manualReportIds : uniqueNumbers(competitorReportIds);

    if (!reportIds.length) {
      throw new Error("No AdPlexity report IDs were found in Airtable Competitors or manual input.");
    }

    const client = new AdplexityClient(config.adplexity.email, config.adplexity.password, log);
    await client.initialize();
    const reports = await client.listReports().catch((error) => {
      log(`Could not load AdPlexity report names: ${error instanceof Error ? error.message : String(error)}`);
      return [];
    });

    const reportNameLookup = new Map(reports.map((report) => [report.id, report.name]));
    for (const reportId of reportIds) {
      log(`Prepared report ${reportId}${reportNameLookup.has(reportId) ? ` (${reportNameLookup.get(reportId)})` : ""}.`);
    }

    const existingKeys = new Set((await airtable.listCheckpoints()).map((item) => item.key));
    const checkpoints = buildInitialReportCheckpoints(reportIds, reportNameLookup);
    const seedResult = await airtable.seedCheckpointsIncremental(checkpoints, existingKeys);

    const summary = {
      inputMode: manualReportIds.length ? "manual-only" : "competitors",
      reportIds: reportIds.length,
      checkpointsPrepared: checkpoints.length,
      checkpointsCreated: seedResult.created,
      checkpointsExisting: seedResult.existing
    };

    log(
      `Seed complete: prepared ${summary.checkpointsPrepared}, created ${summary.checkpointsCreated}, existing ${summary.checkpointsExisting}.`
    );
    return summary;
  } finally {
    runtime.finish("seed-queue");
  }
}

async function processPageCheckpoint({ airtable, checkpoint, client, log, knownKeys }) {
  const reportId = checkpoint.reportId;
  const reportName = checkpoint.reportName || String(reportId);
  const offset = Number.parseInt(String(checkpoint.cursor || "0"), 10) || 0;
  const listings = await client.getReportPage(reportId, offset);

  if (!listings.length) {
    await airtable.completeCheckpoint(checkpoint.id, 0);
    log(`[${reportName}] No ads found on page offset ${offset}.`);
    return { status: "done" };
  }

  const normalizedAds = [];
  const detailRows = [];
  for (const listing of listings) {
    const adId = toAdplexityId(listing.id);
    if (adId === null) {
      continue;
    }
    const normalizedAd = normalizeListingAd(listing);
    if (shouldDebugLandingPage(normalizedAd.landingPageUrl) && !normalizedAd.trackerUrl) {
      const urlLikeEntries = [];
      collectUrlLikeEntries(listing, urlLikeEntries);
      log(
        `[${reportName}] Landing-page debug for ad ${adId}: ` +
          (urlLikeEntries.length ? urlLikeEntries.slice(0, 20).join(" | ") : "no url-like fields found in listing")
      );
    }
    normalizedAds.push(normalizedAd);
    detailRows.push({
      key: buildDetailCheckpointKey(reportId, adId),
      stage: "ad-detail",
      reportId,
      reportName,
      cursor: "",
      adId: String(adId),
      snapshotJson: serializeJson(listing)
    });
  }

  if (normalizedAds.length) {
    await airtable.upsertAds(normalizedAds);
  }

  const rowsToSeed = [...detailRows];
  if (listings.length >= PAGE_SIZE) {
    rowsToSeed.push({
      key: buildPageCheckpointKey(reportId, offset + PAGE_SIZE),
      stage: "report-page",
      reportId,
      reportName,
      cursor: String(offset + PAGE_SIZE),
      adId: "",
      snapshotJson: ""
    });
  }

  const seedResult = await airtable.seedCheckpointsIncremental(rowsToSeed, knownKeys);
  await airtable.completeCheckpoint(checkpoint.id, normalizedAds.length);
  log(
    `[${reportName}] Stored ${normalizedAds.length} listing rows from offset ${offset}. ` +
      `Created ${seedResult.created} follow-up checkpoints, existing ${seedResult.existing}.`
  );
  return { status: "done" };
}

async function processDetailCheckpoint({ airtable, checkpoint, client, log }) {
  const reportName = checkpoint.reportName || String(checkpoint.reportId || "");
  const adId = Number.parseInt(String(checkpoint.adId || ""), 10);
  if (!Number.isFinite(adId) || adId <= 0) {
    throw new Error(`Invalid AdPlexity ad id: ${checkpoint.adId}`);
  }

  const listing = parseJsonObject(checkpoint.snapshotJson);
  const detail = await client.getAdDetail(adId);
  if (!detail) {
    await airtable.completeCheckpoint(checkpoint.id, 0);
    log(`[${reportName}] Ad ${adId} detail not found; marked complete.`);
    return { status: "done" };
  }

  const normalized = normalizeDetailedAd(adId, listing, detail);
  if (!normalized.landingPageUrl && normalized.trackerUrl) {
    const resolvedLandingPage = await client.resolveDestinationUrl(normalized.trackerUrl).catch((error) => {
      log(
        `[${reportName}] Could not resolve tracker URL for ad ${adId}: ${
          error instanceof Error ? error.message : String(error)
        }`
      );
      return "";
    });
    if (resolvedLandingPage && !isAdplexityTrackerUrl(resolvedLandingPage)) {
      normalized.landingPageUrl = resolvedLandingPage;
    }
  }
  if (shouldDebugLandingPage(normalized.landingPageUrl)) {
    const urlLikeEntries = [];
    collectUrlLikeEntries(listing, urlLikeEntries);
    collectUrlLikeEntries(detail, urlLikeEntries);
    log(
      `[${reportName}] Landing-page debug for ad ${adId}: ` +
        (urlLikeEntries.length ? urlLikeEntries.slice(0, 30).join(" | ") : "no url-like fields found")
    );
  }
  await airtable.upsertAds([normalized]);
  await airtable.completeCheckpoint(checkpoint.id, 1);
  log(`[${reportName}] Stored enriched AdPlexity ad ${adId}.`);
  return { status: "done" };
}

async function processCheckpoint(args) {
  const { checkpoint } = args;
  if (checkpoint.stage === "report-page") {
    return processPageCheckpoint(args);
  }
  if (checkpoint.stage === "ad-detail") {
    return processDetailCheckpoint(args);
  }
  throw new Error(`Unsupported checkpoint stage: ${checkpoint.stage || "(blank)"}`);
}

export async function runAdplexityChunk({ airtable, config, runtime, payload }) {
  ensureAdplexityConfigured();
  runtime.begin("run-chunk");
  const log = runtime.log;
  const startedAt = Date.now();

  const maxMsInput = Number.parseInt(String(payload?.maxMs ?? ""), 10);
  const maxCheckpointsInput = Number.parseInt(String(payload?.maxCheckpoints ?? ""), 10);
  const maxMs = Number.isFinite(maxMsInput) && maxMsInput > 0 ? maxMsInput : config.runtime.chunkMaxMs;
  const maxCheckpoints =
    Number.isFinite(maxCheckpointsInput) && maxCheckpointsInput > 0
      ? maxCheckpointsInput
      : config.runtime.chunkMaxCheckpoints;

  const summary = {
    processed: 0,
    done: 0,
    failed: 0,
    reason: "queue-empty",
    continueSuggested: false,
    durationMs: 0
  };

  try {
    const client = new AdplexityClient(config.adplexity.email, config.adplexity.password, log);
    await client.initialize();
    await client.listReports().catch((error) => {
      log(`Could not warm AdPlexity report session: ${error instanceof Error ? error.message : String(error)}`);
      return [];
    });
    const checkpointSnapshot = await airtable.listCheckpoints();
    const knownKeys = new Set(checkpointSnapshot.map((item) => item.key));
    const runnable = selectRunnableCheckpoints(
      checkpointSnapshot,
      config.runtime.retryLimit,
      config.runtime.staleMinutes
    );

    for (const checkpoint of runnable) {
      if (summary.processed >= maxCheckpoints) {
        summary.reason = "checkpoint-budget-reached";
        break;
      }
      if (runtime.state.stopRequested) {
        summary.reason = "stop-requested";
        break;
      }
      if (Date.now() - startedAt >= maxMs && summary.processed > 0) {
        summary.reason = "budget-reached";
        break;
      }

      runtime.setCheckpoint(checkpoint.key);
      const nextAttempt = checkpoint.attemptCount + 1;
      log(`Claimed ${checkpoint.key} (attempt ${nextAttempt}).`);
      await airtable.startCheckpoint(checkpoint.id, nextAttempt);
      checkpoint.status = "running";
      checkpoint.attemptCount = nextAttempt;

      const heartbeatTimer = setInterval(() => {
        airtable.heartbeatCheckpoint(checkpoint.id).catch((error) => {
          log(`Heartbeat failed for ${checkpoint.key}: ${error instanceof Error ? error.message : String(error)}`);
        });
      }, 20000);

      try {
        const result = await processCheckpoint({
          airtable,
          checkpoint,
          client,
          log,
          knownKeys
        });
        summary.processed += 1;
        if (result.status === "done") {
          summary.done += 1;
        } else {
          summary.failed += 1;
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        await airtable.failCheckpoint(checkpoint.id, message);
        summary.processed += 1;
        summary.failed += 1;
        log(`Checkpoint ${checkpoint.key} failed: ${message}`);
      } finally {
        clearInterval(heartbeatTimer);
        runtime.setCheckpoint("");
      }
    }

    summary.durationMs = Date.now() - startedAt;
    summary.continueSuggested = await airtable.hasOutstandingWork({
      retryLimit: config.runtime.retryLimit,
      staleMinutes: config.runtime.staleMinutes
    });
    log(
      `Chunk finished: processed ${summary.processed}, done ${summary.done}, failed ${summary.failed}, reason=${summary.reason}.`
    );
    return summary;
  } finally {
    runtime.finish("run-chunk");
  }
}
