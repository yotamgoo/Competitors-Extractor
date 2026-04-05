import { scrapeMetaAdsSlice } from "./meta-client.js";
import {
  coerceLineList,
  hashText,
  normalizePageId,
  parseJsonObject,
  serializeJson,
  uniqueStrings
} from "./utils.js";

function normalizeMedia(value, fallback = "both") {
  const text = String(value ?? fallback).trim().toLowerCase();
  if (text === "image" || text === "video" || text === "both") {
    return text;
  }
  return fallback;
}

function toPositiveInt(value, fallback, minimum = 1, maximum = 1000) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(minimum, Math.min(maximum, parsed));
}

function buildCursor(cursor = "") {
  return String(cursor ?? "").trim() || "start";
}

function checkpointCursorFragment(cursor) {
  const normalized = buildCursor(cursor);
  return normalized === "start" ? "start" : hashText(normalized);
}

function buildCheckpointKey(pageId, searchQuery, cursor) {
  return `meta|page:${pageId}|query:${hashText(searchQuery)}|cursor:${checkpointCursorFragment(cursor)}`;
}

function isEnvironmentSetupError(message) {
  const text = String(message ?? "").toLowerCase();
  return (
    text.includes("meta_graphql_form_template") ||
    text.includes("meta graphql request failed") ||
    text.includes("meta graphql returned") ||
    text.includes("missing required session fields") ||
    text.includes("refresh meta_graphql_form_template")
  );
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
      if ((left.pageName || "") !== (right.pageName || "")) {
        return (left.pageName || "").localeCompare(right.pageName || "");
      }
      if ((left.pageId || "") !== (right.pageId || "")) {
        return (left.pageId || "").localeCompare(right.pageId || "");
      }
      if ((left.position || 0) !== (right.position || 0)) {
        return (left.position || 0) - (right.position || 0);
      }
      return left.key.localeCompare(right.key);
    });
}

function normalizeMetaAd(record, fallbackBrand = "") {
  const firstSeen = String(record.startedRunningDate ?? "").trim();
  const lastSeen = String(record.scrapedAt ?? "").trim().slice(0, 10);
  const platforms = String(record.platforms ?? "").trim();
  const categories = String(record.categories ?? "").trim();
  const brand = String(record.advertiser ?? "").trim() || fallbackBrand;

  return {
    adId: `meta:${record.libraryId}`,
    adCopy: String(record.adCopy ?? "").trim(),
    adUrl: String(record.adLink ?? "").trim(),
    brand,
    categories,
    country: "US",
    cta: String(record.cta ?? "").trim(),
    daysRunning: Number.isFinite(Number(record.runningDays)) ? Number(record.runningDays) : null,
    firstSeen,
    landingPageUrl: String(record.landingUrl ?? "").trim(),
    lastSeen,
    mediaUrl: String(record.mediaUrl ?? "").trim(),
    platforms,
    productCategory: categories,
    status: Number(record.runningDays) > 0 ? "active" : "inactive",
    title: String(record.headline ?? "").trim(),
    winner: false
  };
}

function buildSeedOptions(config, payload) {
  return {
    searchQuery: String(payload?.searchQuery ?? "").trim(),
    minDays: toPositiveInt(payload?.minDays, config.runtime.minDays, 0, 365),
    media: normalizeMedia(payload?.media, config.runtime.media),
    maxAdsPerPage: toPositiveInt(payload?.maxAdsPerPage, config.runtime.maxAdsPerPage, 1, 500),
    sliceMaxAds: toPositiveInt(config.runtime.sliceMaxAds, 30, 1, 100)
  };
}

export async function seedMetaQueue({ airtable, config, runtime, payload }) {
  runtime.begin("seed-queue");
  const log = runtime.log;

  try {
    const manualPageIds = uniqueStrings(
      coerceLineList(payload?.manualPageIds)
        .map((value) => normalizePageId(value))
        .filter(Boolean)
    );
    const seedOptions = buildSeedOptions(config, payload);
    const useManual = manualPageIds.length > 0;

    const pageSeeds = useManual
      ? manualPageIds.map((pageId) => ({
          pageId,
          pageName: pageId
        }))
      : await airtable.getCompetitorPageSeeds();

    if (!pageSeeds.length) {
      throw new Error("No Meta page IDs were found in Airtable Competitors or manual input.");
    }

    const existingKeys = new Set((await airtable.listCheckpoints()).map((item) => item.key));
    const rows = pageSeeds.map((page) => ({
      key: buildCheckpointKey(page.pageId, seedOptions.searchQuery, buildCursor()),
      stage: "page-scan",
      pageId: page.pageId,
      pageName: page.pageName || page.pageId,
      searchQuery: seedOptions.searchQuery,
      cursor: buildCursor(),
      position: 0,
      lastLibraryId: "",
      snapshotJson: serializeJson(seedOptions)
    }));

    for (const page of pageSeeds) {
      log(`Prepared Meta page ${page.pageId}${page.pageName ? ` (${page.pageName})` : ""}.`);
    }

    const seedResult = await airtable.seedCheckpointsIncremental(rows, existingKeys);
    const summary = {
      inputMode: useManual ? "manual-only" : "competitors",
      pages: pageSeeds.length,
      searchQuery: seedOptions.searchQuery,
      minDays: seedOptions.minDays,
      media: seedOptions.media,
      maxAdsPerPage: seedOptions.maxAdsPerPage,
      checkpointsPrepared: rows.length,
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

async function processPageCheckpoint({ airtable, checkpoint, config, log, knownKeys }) {
  const snapshot = parseJsonObject(checkpoint.snapshotJson) || {};
  const searchQuery = String(checkpoint.searchQuery || snapshot.searchQuery || "").trim();
  const minDays = toPositiveInt(snapshot.minDays, config.runtime.minDays, 0, 365);
  const media = normalizeMedia(snapshot.media, config.runtime.media);
  const maxAdsPerPage = toPositiveInt(snapshot.maxAdsPerPage, config.runtime.maxAdsPerPage, 1, 500);
  const sliceMaxAds = toPositiveInt(snapshot.sliceMaxAds, config.runtime.sliceMaxAds, 1, 100);

  const label = checkpoint.pageName || checkpoint.pageId;
  const result = await scrapeMetaAdsSlice({
    pageId: checkpoint.pageId,
    searchQuery,
    minDays,
    media,
    maxAdsPerPage,
    startPosition: checkpoint.position,
    cursor: checkpoint.cursor,
    lastLibraryId: checkpoint.lastLibraryId,
    sliceMaxAds,
    apiConfig: config.metaApi,
    log
  });

  const normalizedAds = result.ads.map((ad) =>
    normalizeMetaAd(ad, result.pageName || checkpoint.pageName || checkpoint.pageId)
  );
  if (normalizedAds.length) {
    await airtable.upsertAds(normalizedAds);
  }

  let seedResult = { created: 0, existing: 0 };
  const advanced = result.nextPosition > checkpoint.position;
  const nextCursor = String(result.nextCursor ?? "").trim();
  const normalizedNextCursor = buildCursor(nextCursor);
  const cursorAdvanced = Boolean(nextCursor) && normalizedNextCursor !== buildCursor(checkpoint.cursor);
  const nextPosition = result.nextPosition;
  const nextPageName = result.pageName || checkpoint.pageName || checkpoint.pageId;
  if (!result.reachedEnd && nextCursor && (advanced || cursorAdvanced)) {
    seedResult = await airtable.seedCheckpointsIncremental(
      [
        {
          key: buildCheckpointKey(checkpoint.pageId, searchQuery, normalizedNextCursor),
          stage: "page-scan",
          pageId: checkpoint.pageId,
          pageName: nextPageName,
          searchQuery,
          cursor: normalizedNextCursor,
          position: nextPosition,
          lastLibraryId: result.lastLibraryId,
          snapshotJson: checkpoint.snapshotJson
        }
      ],
      knownKeys
    );
  }

  await airtable.completeCheckpoint(checkpoint.id, normalizedAds.length);

  if (!result.reachedEnd && !cursorAdvanced) {
    log(`[${label}] Slice did not advance cursor from ${buildCursor(checkpoint.cursor)}; stopping this branch.`);
  }

  log(
    `[${label}] Stored ${normalizedAds.length} ads from position ${checkpoint.position}. ` +
      `next=${result.nextPosition} cursor=${nextCursor || "(end)"} reason=${result.breakReason}. ` +
      `created_next=${seedResult.created} existing_next=${seedResult.existing}.`
  );
  return { status: "done" };
}

async function processCheckpoint(args) {
  if (args.checkpoint.stage === "page-scan") {
    return processPageCheckpoint(args);
  }
  throw new Error(`Unsupported Meta checkpoint stage: ${args.checkpoint.stage || "(blank)"}`);
}

export async function runMetaChunk({ airtable, config, runtime, payload }) {
  runtime.begin("run-chunk");
  const log = runtime.log;
  const startedAt = Date.now();
  let environmentError = false;

  const maxMs = toPositiveInt(payload?.maxMs, config.runtime.chunkMaxMs, 1000, 3600000);
  const maxCheckpoints = toPositiveInt(
    payload?.maxCheckpoints,
    config.runtime.chunkMaxCheckpoints,
    1,
    1000
  );

  const summary = {
    processed: 0,
    done: 0,
    failed: 0,
    reason: "queue-empty",
    continueSuggested: false,
    durationMs: 0
  };

  try {
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
          config,
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
        if (isEnvironmentSetupError(message)) {
          environmentError = true;
          summary.reason = "environment-error";
        }
      } finally {
        clearInterval(heartbeatTimer);
        runtime.setCheckpoint("");
      }

      if (environmentError) {
        log("Stopping auto-run after environment/setup failure so retries are not burned unnecessarily.");
        break;
      }
    }

    summary.durationMs = Date.now() - startedAt;
    summary.continueSuggested = environmentError
      ? false
      : await airtable.hasOutstandingWork({
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
