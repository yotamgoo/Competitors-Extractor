import { ensureForeplayConfigured } from "./config.js";
import { ForeplayClient } from "./foreplay-client.js";
import {
  cleanHtmlText,
  coerceLineList,
  lookbackStartMs,
  normalizeDaysRunning,
  normalizeDateTime,
  normalizePlatformList,
  parseDateBucketTimestamp,
  toUtcDayRange,
  uniqueStrings
} from "./utils.js";

function buildCheckpointKey(brandId, bucketDate) {
  return `foreplay|${brandId}|${bucketDate}`;
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
      return (left.bucketDate || "").localeCompare(right.bucketDate || "") || left.key.localeCompare(right.key);
    });
}

function hasInlineMedia(ad) {
  const firstCard = Array.isArray(ad.cards) ? ad.cards[0] : null;
  return Boolean(firstCard?.video || firstCard?.thumbnail || firstCard?.image || ad.image);
}

function normalizeForeplayWinner(ad, brandName) {
  const firstCard = Array.isArray(ad.cards) ? ad.cards[0] : null;
  const description = cleanHtmlText(ad.description || firstCard?.description || "");
  const imageUrl = firstCard?.thumbnail || firstCard?.image || ad.image || ad.avatar || "";
  const videoUrl = firstCard?.video || "";
  const title = String(ad.headline || ad.name || "").trim();
  const cta = String(firstCard?.cta_text || ad.cta_title || ad.cta_type || "").trim();
  const firstSeen = normalizeDateTime(ad.startedRunning);
  const lastSeen = normalizeDateTime(ad.end_date);
  const mediaUrl = videoUrl || imageUrl || "";
  const platforms = normalizePlatformList(ad.publisher_platform || []).map((platform) =>
    platform
      .split("_")
      .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
      .join(" ")
  );

  return {
    adId: `foreplay:${String(ad.ad_id || ad.id || "").trim()}`,
    adCopy: description,
    adUrl: ad.ad_id ? `https://www.facebook.com/ads/library/?id=${ad.ad_id}` : "",
    brand: String(brandName || "").trim(),
    categories: [],
    country: "",
    cta,
    daysRunning: normalizeDaysRunning(null, {
      firstSeen: ad.startedRunning,
      lastSeen: ad.end_date
    }),
    firstSeen: firstSeen ? firstSeen.slice(0, 10) : "",
    landingPageUrl: String(ad.link_url || "").trim(),
    lastSeen: lastSeen ? lastSeen.slice(0, 10) : "",
    mediaUrl,
    platforms,
    productCategory: "",
    status: ad.live ? "active" : "inactive",
    title,
    winner: true
  };
}

async function resolveBrandReferences(client, references, log) {
  const resolved = [];

  for (const reference of references) {
    const input = String(reference ?? "").trim();
    if (!input) {
      continue;
    }

    if (/^\d+$/.test(input)) {
      let brandId = await client.resolveBrandIdFromPageId(input);
      let brandName = brandId ? await client.resolveBrandNameFromBrandId(brandId) : null;

      if (!brandId) {
        log(`Skipping page ID ${input}: not found in Foreplay.`);
        continue;
      }
      if (!brandName) {
        brandName = (await client.resolveBrandNameFromBrandId(brandId)) || brandId;
      }

      resolved.push({ input, brandId, brandName });
      log(`Resolved Foreplay reference ${input} -> ${brandName} (${brandId})`);
      continue;
    }

    const brandName = (await client.resolveBrandNameFromBrandId(input)) || input;
    resolved.push({ input, brandId: input, brandName });
    log(`Using Foreplay brand ${brandName} (${input})`);
  }
  return resolved;
}

export async function seedForeplayQueue({ airtable, config, runtime, payload }) {
  ensureForeplayConfigured();
  runtime.begin("seed-queue");
  const log = runtime.log;

  try {
    const monthsInput = Number.parseInt(String(payload?.months ?? "3"), 10);
    const months = Number.isFinite(monthsInput) && monthsInput > 0 ? monthsInput : 3;
    const manualBrandIds = coerceLineList(payload?.manualBrandIds);
    const competitorBrandIds = manualBrandIds.length ? [] : await airtable.getCompetitorBrandIds();
    const references = manualBrandIds.length
      ? uniqueStrings(manualBrandIds)
      : uniqueStrings(competitorBrandIds);

    if (!references.length) {
      throw new Error("No Foreplay brand IDs were found in Airtable Competitors or manual input.");
    }

    const client = new ForeplayClient(config.foreplay.email, config.foreplay.password, log);
    await client.initialize();
    const resolved = await resolveBrandReferences(client, references, log);
    const cutoffMs = lookbackStartMs(months);
    const existingKeys = new Set((await airtable.listCheckpoints()).map((item) => item.key));

    let checkpointsPrepared = 0;
    let checkpointsCreated = 0;
    let checkpointsExisting = 0;
    for (const brand of resolved) {
      log(`Loading creative-test buckets for ${brand.brandName}...`);
      const aggregations = await client.getCreativeTestDates(brand.brandId);
      const dates = aggregations
        .filter((item) => Number(item?.liveCount ?? 0) === 1)
        .map((item) => parseDateBucketTimestamp(item.date))
        .filter((timestamp) => Number.isFinite(timestamp) && timestamp >= cutoffMs)
        .map((timestamp) => new Date(timestamp).toISOString().slice(0, 10));

      const uniqueDates = uniqueStrings(dates);
      if (!uniqueDates.length) {
        log(`No eligible winner buckets found for ${brand.brandName}.`);
        continue;
      }

      const checkpoints = [];
      for (const bucketDate of uniqueDates) {
        checkpoints.push({
          key: buildCheckpointKey(brand.brandId, bucketDate),
          brandId: brand.brandId,
          brandName: brand.brandName,
          bucketDate
        });
      }

      checkpointsPrepared += checkpoints.length;
      const seedResult = await airtable.seedCheckpointsIncremental(checkpoints, existingKeys);
      checkpointsCreated += seedResult.created;
      checkpointsExisting += seedResult.existing;
      log(
        `Prepared ${checkpoints.length} checkpoints for ${brand.brandName}. Created ${seedResult.created}, existing ${seedResult.existing}.`
      );
    }

    const summary = {
      months,
      references: references.length,
      inputMode: manualBrandIds.length ? "manual-only" : "competitors",
      resolvedBrands: resolved.length,
      checkpointsPrepared,
      checkpointsCreated,
      checkpointsExisting
    };
    log(
      `Seed complete: prepared ${summary.checkpointsPrepared}, created ${summary.checkpointsCreated}, existing ${summary.checkpointsExisting}.`
    );
    return summary;
  } finally {
    runtime.finish("seed-queue");
  }
}

async function processCheckpoint({ airtable, checkpoint, client, log }) {
  const range = toUtcDayRange(checkpoint.bucketDate);
  log(`Processing ${checkpoint.key}...`);

  const dayAds = [];
  for await (const ad of client.iterAds({
    brandId: checkpoint.brandId,
    startedAfter: range.start,
    startedBefore: range.end
  })) {
    dayAds.push(ad);
  }

  const liveAds = dayAds.filter((ad) => Boolean(ad.live));
  if (liveAds.length !== 1) {
    const reason =
      liveAds.length > 1
        ? `Multiple live ads found for ${checkpoint.key}; creative test may still be in progress.`
        : `No single live winner found for ${checkpoint.key}.`;
    await airtable.failCheckpoint(checkpoint.id, reason);
    log(reason);
    return { status: "failed" };
  }

  const winner = { ...liveAds[0] };
  if (!hasInlineMedia(winner)) {
    const thumbnailUrl = await client.getDcoThumbnail({
      brandId: checkpoint.brandId,
      collationId: winner.collationId || null,
      fbAdId: winner.ad_id || null,
      startedRunning: winner.startedRunning || null
    });

    if (thumbnailUrl) {
      if (!Array.isArray(winner.cards) || !winner.cards.length) {
        winner.cards = [{}];
      }
      winner.cards[0] = {
        ...(winner.cards[0] || {}),
        image: thumbnailUrl
      };
    }
  }

  const normalized = normalizeForeplayWinner(winner, checkpoint.brandName);
  await airtable.upsertAds([normalized]);
  await airtable.completeCheckpoint(checkpoint.id, 1);
  log(`Stored winner for ${checkpoint.brandName} on ${checkpoint.bucketDate}.`);
  return { status: "done" };
}

export async function runForeplayChunk({ airtable, config, runtime, payload }) {
  ensureForeplayConfigured();
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
    const client = new ForeplayClient(config.foreplay.email, config.foreplay.password, log);
    await client.initialize();
    const checkpointSnapshot = await airtable.listCheckpoints();
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
      log(`Claimed ${checkpoint.key} (attempt ${checkpoint.attemptCount}).`);
      await airtable.updateRecord(config.airtable.checkpointsTable, checkpoint.id, {
        Status: "running",
        "Attempt Count": checkpoint.attemptCount + 1,
        "Last Error": "",
        "Heartbeat At": new Date().toISOString().replace(/\.\d{3}Z$/, "Z")
      });
      checkpoint.status = "running";
      checkpoint.attemptCount += 1;

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
          log
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
