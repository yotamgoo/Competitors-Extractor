import { ensureAirtableConfigured } from "./config.js";
import { isTruthyLike, normalizeDateOnly, nowIso, sleep, uniqueStrings } from "./utils.js";

const BASE_URL = "https://api.airtable.com/v0/";
const MAX_BATCH = 10;
const MAX_RETRIES = 4;

const CHECKPOINT_FIELDS = {
  key: "Checkpoint Key",
  status: "Status",
  brandId: "Brand Id",
  brandName: "Brand Name",
  bucketDate: "Bucket Date",
  attemptCount: "Attempt Count",
  adsWritten: "Ads Written",
  lastError: "Last Error",
  heartbeatAt: "Heartbeat At",
  completedAt: "Completed At"
};

function chunk(items, size) {
  const batches = [];
  for (let index = 0; index < items.length; index += size) {
    batches.push(items.slice(index, index + size));
  }
  return batches;
}

function encodeTablePath(tableName) {
  return tableName
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
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

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function compactFields(fields) {
  return Object.fromEntries(
    Object.entries(fields).filter(([, value]) => value !== "" && value !== null && value !== undefined)
  );
}

function normalizeCheckpoint(record) {
  const fields = record.fields || {};
  return {
    id: record.id,
    key: firstText(fields[CHECKPOINT_FIELDS.key], record.id),
    status: firstText(fields[CHECKPOINT_FIELDS.status], "pending").toLowerCase() || "pending",
    brandId: firstText(fields[CHECKPOINT_FIELDS.brandId]),
    brandName: firstText(fields[CHECKPOINT_FIELDS.brandName]),
    bucketDate: normalizeDateOnly(fields[CHECKPOINT_FIELDS.bucketDate]),
    attemptCount: toNumber(fields[CHECKPOINT_FIELDS.attemptCount], 0),
    adsWritten: toNumber(fields[CHECKPOINT_FIELDS.adsWritten], 0),
    lastError: firstText(fields[CHECKPOINT_FIELDS.lastError]),
    heartbeatAt: firstText(fields[CHECKPOINT_FIELDS.heartbeatAt]),
    completedAt: firstText(fields[CHECKPOINT_FIELDS.completedAt])
  };
}

export class AirtableClient {
  constructor(config, log = console.log) {
    this.config = config;
    this.log = log;
    this.adsRecordIdByAdId = null;
  }

  ensureConfigured() {
    ensureAirtableConfigured();
  }

  async getCompetitorBrandIds() {
    this.ensureConfigured();
    const records = await this.listRecords(this.config.airtable.competitorsTable);
    const brandIds = [];

    for (const record of records) {
      const fields = record.fields || {};
      if (!isTruthyLike(fields[this.config.airtable.competitorsActiveField])) {
        continue;
      }
      const raw = fields[this.config.airtable.competitorsForeplayField];
      if (raw === undefined || raw === null || raw === "") {
        continue;
      }

      const pieces = Array.isArray(raw)
        ? raw.flatMap((item) => String(item ?? "").split(/[,\n;]/))
        : String(raw).split(/[,\n;]/);
      for (const piece of pieces) {
        const value = String(piece ?? "").trim();
        if (value) {
          brandIds.push(value);
        }
      }
    }

    return uniqueStrings(brandIds);
  }

  async listCheckpoints() {
    this.ensureConfigured();
    const records = await this.listRecords(this.config.airtable.checkpointsTable);
    return records.map(normalizeCheckpoint);
  }

  async getCheckpointState({ retryLimit, staleMinutes }) {
    const checkpoints = await this.listCheckpoints();
    const staleBefore = Date.now() - staleMinutes * 60 * 1000;
    let pending = 0;
    let running = 0;
    let done = 0;
    let failed = 0;
    let staleRunning = 0;
    let retryableFailed = 0;

    for (const checkpoint of checkpoints) {
      if (checkpoint.status === "pending") {
        pending += 1;
      } else if (checkpoint.status === "running") {
        running += 1;
        const heartbeatMs = checkpoint.heartbeatAt ? Date.parse(checkpoint.heartbeatAt) : 0;
        if (heartbeatMs && heartbeatMs < staleBefore) {
          staleRunning += 1;
        }
      } else if (checkpoint.status === "done") {
        done += 1;
      } else if (checkpoint.status === "failed") {
        failed += 1;
        if (checkpoint.attemptCount < retryLimit) {
          retryableFailed += 1;
        }
      }
    }

    const sorted = [...checkpoints].sort((left, right) => {
      if (left.status !== right.status) {
        return left.status.localeCompare(right.status);
      }
      return (right.bucketDate || "").localeCompare(left.bucketDate || "") || left.key.localeCompare(right.key);
    });

    return {
      total: checkpoints.length,
      pending,
      running,
      done,
      failed,
      staleRunning,
      retryableFailed,
      checkpoints: sorted.slice(0, 200)
    };
  }

  async seedCheckpoints(rows) {
    const existing = await this.listCheckpoints();
    const existingKeys = new Set(existing.map((item) => item.key));
    const creates = [];

    for (const row of rows) {
      if (existingKeys.has(row.key)) {
        continue;
      }
      creates.push({
        fields: compactFields({
          [CHECKPOINT_FIELDS.key]: row.key,
          [CHECKPOINT_FIELDS.status]: "pending",
          [CHECKPOINT_FIELDS.brandId]: row.brandId,
          [CHECKPOINT_FIELDS.brandName]: row.brandName,
          [CHECKPOINT_FIELDS.bucketDate]: row.bucketDate,
          [CHECKPOINT_FIELDS.attemptCount]: 0,
          [CHECKPOINT_FIELDS.adsWritten]: 0,
          [CHECKPOINT_FIELDS.lastError]: ""
        })
      });
    }

    let created = 0;
    for (const batch of chunk(creates, MAX_BATCH)) {
      await this.requestJson("POST", this.config.airtable.checkpointsTable, {
        body: {
          records: batch,
          typecast: true
        }
      });
      created += batch.length;
    }

    return {
      created,
      existing: rows.length - created
    };
  }

  async seedCheckpointsIncremental(rows, existingKeys = null) {
    const knownKeys = existingKeys ?? new Set((await this.listCheckpoints()).map((item) => item.key));
    const creates = [];
    let created = 0;
    let existing = 0;

    for (const row of rows) {
      if (knownKeys.has(row.key)) {
        existing += 1;
        continue;
      }

      knownKeys.add(row.key);
      creates.push({
        fields: compactFields({
          [CHECKPOINT_FIELDS.key]: row.key,
          [CHECKPOINT_FIELDS.status]: "pending",
          [CHECKPOINT_FIELDS.brandId]: row.brandId,
          [CHECKPOINT_FIELDS.brandName]: row.brandName,
          [CHECKPOINT_FIELDS.bucketDate]: row.bucketDate,
          [CHECKPOINT_FIELDS.attemptCount]: 0,
          [CHECKPOINT_FIELDS.adsWritten]: 0,
          [CHECKPOINT_FIELDS.lastError]: ""
        })
      });
    }

    for (const batch of chunk(creates, MAX_BATCH)) {
      await this.requestJson("POST", this.config.airtable.checkpointsTable, {
        body: {
          records: batch,
          typecast: true
        }
      });
      created += batch.length;
    }

    return {
      created,
      existing,
      knownKeys
    };
  }

  async claimNextCheckpoint({ retryLimit, staleMinutes }) {
    const checkpoints = await this.listCheckpoints();
    const staleBefore = Date.now() - staleMinutes * 60 * 1000;
    const candidates = checkpoints
      .filter((checkpoint) => {
        if (checkpoint.status === "done") {
          return false;
        }
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

    if (!candidates.length) {
      return null;
    }

    const chosen = candidates[0];
    await this.updateRecord(this.config.airtable.checkpointsTable, chosen.id, {
      [CHECKPOINT_FIELDS.status]: "running",
      [CHECKPOINT_FIELDS.attemptCount]: chosen.attemptCount + 1,
      [CHECKPOINT_FIELDS.lastError]: "",
      [CHECKPOINT_FIELDS.heartbeatAt]: nowIso()
    });

    return {
      ...chosen,
      status: "running",
      attemptCount: chosen.attemptCount + 1,
      heartbeatAt: nowIso()
    };
  }

  async heartbeatCheckpoint(recordId) {
    await this.updateRecord(this.config.airtable.checkpointsTable, recordId, {
      [CHECKPOINT_FIELDS.heartbeatAt]: nowIso()
    });
  }

  async completeCheckpoint(recordId, adsWritten) {
    await this.updateRecord(this.config.airtable.checkpointsTable, recordId, {
      [CHECKPOINT_FIELDS.status]: "done",
      [CHECKPOINT_FIELDS.adsWritten]: adsWritten,
      [CHECKPOINT_FIELDS.lastError]: "",
      [CHECKPOINT_FIELDS.heartbeatAt]: nowIso(),
      [CHECKPOINT_FIELDS.completedAt]: nowIso()
    });
  }

  async failCheckpoint(recordId, message) {
    await this.updateRecord(this.config.airtable.checkpointsTable, recordId, {
      [CHECKPOINT_FIELDS.status]: "failed",
      [CHECKPOINT_FIELDS.lastError]: String(message ?? "").slice(0, 100000),
      [CHECKPOINT_FIELDS.heartbeatAt]: nowIso()
    });
  }

  async hasOutstandingWork({ retryLimit, staleMinutes }) {
    const state = await this.getCheckpointState({ retryLimit, staleMinutes });
    return state.pending > 0 || state.retryableFailed > 0 || state.staleRunning > 0;
  }

  async clearCheckpoints() {
    this.ensureConfigured();
    const checkpoints = await this.listCheckpoints();
    if (!checkpoints.length) {
      return { deleted: 0 };
    }

    let deleted = 0;
    for (const batch of chunk(checkpoints.map((checkpoint) => checkpoint.id), MAX_BATCH)) {
      await this.requestJson("DELETE", this.config.airtable.checkpointsTable, {
        query: {
          "records[]": batch
        }
      });
      deleted += batch.length;
    }

    return { deleted };
  }

  async upsertAds(ads) {
    this.ensureConfigured();
    if (!ads.length) {
      return { created: 0, updated: 0 };
    }

    const existingById = await this.getAdsRecordIdByAdId();

    const creates = [];
    const updates = [];
    for (const ad of ads) {
      const fields = compactFields({
        "Ad Id": ad.adId,
        "Ad Copy": ad.adCopy,
        "Ad URL": ad.adUrl,
        Brand: ad.brand,
        Categories: ad.categories,
        Country: ad.country,
        CTA: ad.cta,
        "Days Running": ad.daysRunning,
        Duplicates: 0,
        "First Seen": ad.firstSeen,
        "Landing Page URL": ad.landingPageUrl,
        "Last Seen": ad.lastSeen,
        "Media URL": ad.mediaUrl,
        Platforms: ad.platforms,
        "Product Category": ad.productCategory,
        Status: ad.status,
        Title: ad.title,
        Winner: ad.winner
      });

      const existingRecordId = existingById.get(ad.adId);
      if (existingRecordId) {
        updates.push({ id: existingRecordId, fields });
      } else {
        creates.push({ fields });
      }
    }

    let created = 0;
    for (const batch of chunk(creates, MAX_BATCH)) {
      const payload = await this.requestJson("POST", this.config.airtable.adsTable, {
        body: {
          records: batch,
          typecast: true
        }
      });
      this.rememberCreatedAds(Array.isArray(payload.records) ? payload.records : []);
      created += batch.length;
    }

    let updated = 0;
    for (const batch of chunk(updates, MAX_BATCH)) {
      await this.requestJson("PATCH", this.config.airtable.adsTable, {
        body: {
          records: batch,
          typecast: true
        }
      });
      updated += batch.length;
    }

    return { created, updated };
  }

  async getAdsRecordIdByAdId() {
    if (this.adsRecordIdByAdId) {
      return this.adsRecordIdByAdId;
    }

    const existing = await this.listRecords(this.config.airtable.adsTable);
    const map = new Map();
    for (const record of existing) {
      const adId = firstText(record.fields?.["Ad Id"]);
      if (adId) {
        map.set(adId, record.id);
      }
    }

    this.adsRecordIdByAdId = map;
    return map;
  }

  rememberCreatedAds(createdItems) {
    if (!this.adsRecordIdByAdId) {
      return;
    }

    for (const item of createdItems) {
      const adId = firstText(item.fields?.["Ad Id"]);
      if (adId && item.id) {
        this.adsRecordIdByAdId.set(adId, item.id);
      }
    }
  }

  async listRecords(tableName) {
    const records = [];
    let offset = "";
    while (true) {
      const query = { pageSize: 100 };
      if (offset) {
        query.offset = offset;
      }

      const payload = await this.requestJson("GET", tableName, { query });
      if (Array.isArray(payload.records)) {
        records.push(...payload.records);
      }
      if (!payload.offset) {
        break;
      }
      offset = payload.offset;
    }
    return records;
  }

  async updateRecord(tableName, recordId, fields) {
    await this.requestJson("PATCH", tableName, {
      body: {
        records: [{ id: recordId, fields }],
        typecast: true
      }
    });
  }

  async requestJson(method, tableName, options = {}) {
    this.ensureConfigured();

    const path = `${encodeURIComponent(this.config.airtable.baseId)}/${encodeTablePath(tableName)}`;
    const url = new URL(path, BASE_URL);
    for (const [key, value] of Object.entries(options.query || {})) {
      if (value === undefined || value === null || value === "") {
        continue;
      }
      if (Array.isArray(value)) {
        for (const item of value) {
          if (item === undefined || item === null || item === "") {
            continue;
          }
          url.searchParams.append(key, String(item));
        }
        continue;
      }
      url.searchParams.set(key, String(value));
    }

    const headers = {
      authorization: `Bearer ${this.config.airtable.token}`,
      "content-type": "application/json"
    };

    let lastError = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      try {
        const response = await fetch(url, {
          method,
          headers,
          body: options.body ? JSON.stringify(options.body) : undefined,
          signal: AbortSignal.timeout(30000)
        });

        if (response.status === 429 || response.status >= 500) {
          lastError = new Error(`Airtable temporary error ${response.status}`);
          await sleep(2 ** attempt * 1000);
          continue;
        }

        if (!response.ok) {
          const message = (await response.text().catch(() => "")).slice(0, 500);
          throw new Error(`Airtable request failed (${response.status}): ${message}`);
        }

        return await response.json().catch(() => ({}));
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt >= MAX_RETRIES) {
          break;
        }
        await sleep(2 ** attempt * 1000);
      }
    }

    throw lastError || new Error("Airtable request failed.");
  }
}
