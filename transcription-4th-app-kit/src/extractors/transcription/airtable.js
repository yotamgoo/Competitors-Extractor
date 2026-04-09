import { ensureAirtableConfigured } from "./config.js";
import { excerpt, firstText, normalizeStatus, nowIso, sleep, toInt } from "./utils.js";

const BASE_URL = "https://api.airtable.com/v0/";
const MAX_BATCH = 10;
const MAX_RETRIES = 4;

function encodeTablePath(tableName) {
  return tableName
    .split("/")
    .map((part) => encodeURIComponent(part))
    .join("/");
}

function chunk(items, size) {
  const batches = [];
  for (let index = 0; index < items.length; index += size) {
    batches.push(items.slice(index, index + size));
  }
  return batches;
}

function firstAttachment(value) {
  if (!Array.isArray(value)) {
    return null;
  }

  for (const item of value) {
    const url = firstText(item?.url);
    if (!url) {
      continue;
    }
    return {
      url,
      filename: firstText(item?.filename),
      type: firstText(item?.type),
      size: toInt(item?.size, 0)
    };
  }

  return null;
}

function inferAttachmentMimeType(attachment) {
  const type = firstText(attachment?.type).toLowerCase();
  if (type.startsWith("image/") || type.startsWith("video/")) {
    return type;
  }

  const filename = firstText(attachment?.filename).toLowerCase();
  if (filename.endsWith(".png")) {
    return "image/png";
  }
  if (filename.endsWith(".jpg") || filename.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (filename.endsWith(".webp")) {
    return "image/webp";
  }
  if (filename.endsWith(".gif")) {
    return "image/gif";
  }
  if (filename.endsWith(".mp4")) {
    return "video/mp4";
  }
  if (filename.endsWith(".mov")) {
    return "video/quicktime";
  }
  if (filename.endsWith(".webm")) {
    return "video/webm";
  }

  const url = firstText(attachment?.url).toLowerCase();
  if (url.includes(".png")) {
    return "image/png";
  }
  if (url.includes(".jpg") || url.includes(".jpeg")) {
    return "image/jpeg";
  }
  if (url.includes(".webp")) {
    return "image/webp";
  }
  if (url.includes(".gif")) {
    return "image/gif";
  }
  if (url.includes(".mp4")) {
    return "video/mp4";
  }
  if (url.includes(".mov")) {
    return "video/quicktime";
  }
  if (url.includes(".webm")) {
    return "video/webm";
  }

  return "application/octet-stream";
}

function isImageLikeAttachment(attachment) {
  return inferAttachmentMimeType(attachment).startsWith("image/");
}

function isNoSpeechTranscript(value) {
  return firstText(value).trim().toUpperCase() === "NO_SPEECH";
}

function compareByUpdated(left, right) {
  const leftMs = left.updatedAt ? Date.parse(left.updatedAt) : 0;
  const rightMs = right.updatedAt ? Date.parse(right.updatedAt) : 0;
  return leftMs - rightMs;
}

function firstLinkedRecordId(value) {
  if (Array.isArray(value)) {
    return firstText(value[0]);
  }
  return firstText(value);
}

function looksLikeAirtableRecordId(value) {
  return /^rec[a-zA-Z0-9]{14,}$/.test(String(value ?? "").trim());
}

function isRetryable(record, retryLimit) {
  return record.status === "failed" && record.attemptCount < retryLimit;
}

function isStaleRunning(record, staleBeforeMs) {
  return record.status === "running" && (!record.heartbeatMs || record.heartbeatMs < staleBeforeMs);
}

function isEligibleQueueRecord(record, { retryLimit, staleBeforeMs }) {
  if (!record.adId && !record.adsRecordId && !record.linkedAdRecordId) {
    return false;
  }
  if (record.status === "running") {
    return isStaleRunning(record, staleBeforeMs);
  }
  if (record.status === "failed") {
    return isRetryable(record, retryLimit);
  }
  return record.status === "pending";
}

function claimPriority(record, staleBeforeMs) {
  if (record.status === "pending") {
    return 0;
  }
  if (isStaleRunning(record, staleBeforeMs)) {
    return 1;
  }
  if (record.status === "failed") {
    return 2;
  }
  return 9;
}

function escapeFormulaValue(value) {
  return String(value ?? "").replaceAll("\\", "\\\\").replaceAll("'", "\\'");
}

export class AirtableClient {
  constructor(config, log = console.log) {
    this.config = config;
    this.log = log;
  }

  ensureConfigured() {
    ensureAirtableConfigured();
  }

  normalizeSourceAd(record) {
    const fields = record.fields || {};
    const attachment = firstAttachment(fields[this.config.airtable.ads.attachmentField]);
    const transcript = firstText(fields[this.config.airtable.ads.transcriptField]);

    return {
      recordId: record.id,
      adId: firstText(fields[this.config.airtable.ads.adIdField]),
      attachment,
      transcript,
      attachmentName: firstText(attachment?.filename),
      attachmentUrl: firstText(attachment?.url)
    };
  }

  normalizeQueueRecord(record) {
    const fields = record.fields || {};
    const heartbeatAt = firstText(fields[this.config.airtable.queue.heartbeatField]);
    const linked = this.config.airtable.queue.adLinkField ? fields[this.config.airtable.queue.adLinkField] : null;
    const linkedFieldText = Array.isArray(linked) ? "" : firstText(linked);
    const linkedAdRecordId = Array.isArray(linked)
      ? firstLinkedRecordId(linked)
      : looksLikeAirtableRecordId(linkedFieldText)
        ? linkedFieldText
        : "";
    const adIdFromLinkField = linkedFieldText && !looksLikeAirtableRecordId(linkedFieldText) ? linkedFieldText : "";

    return {
      recordId: record.id,
      adId: firstText(fields[this.config.airtable.queue.adIdField], adIdFromLinkField),
      adsRecordId: firstText(fields[this.config.airtable.queue.adsRecordIdField], linkedAdRecordId),
      linkedAdRecordId,
      transcript: firstText(fields[this.config.airtable.queue.transcriptField]),
      status: normalizeStatus(fields[this.config.airtable.queue.statusField], "pending"),
      error: firstText(fields[this.config.airtable.queue.errorField]),
      attemptCount: toInt(fields[this.config.airtable.queue.attemptCountField], 0),
      updatedAt: firstText(fields[this.config.airtable.queue.updatedAtField]),
      heartbeatAt,
      heartbeatMs: heartbeatAt ? Date.parse(heartbeatAt) : 0,
      completedAt: firstText(fields[this.config.airtable.queue.completedAtField]),
      seededAt: firstText(fields[this.config.airtable.queue.seededAtField]),
      language: firstText(fields[this.config.airtable.queue.languageField]),
      hasAdLink: Boolean(linkedAdRecordId),
      key: `queue:${record.id}`,
      label: firstText(fields[this.config.airtable.queue.adIdField], adIdFromLinkField, linkedAdRecordId, record.id)
    };
  }

  buildQueueFields(values = {}) {
    const fields = {};
    const mappings = [
      [this.config.airtable.queue.adIdField, values.adId],
      [this.config.airtable.queue.adsRecordIdField, values.adsRecordId],
      [this.config.airtable.queue.transcriptField, values.transcript],
      [this.config.airtable.queue.statusField, values.status],
      [this.config.airtable.queue.errorField, values.error],
      [this.config.airtable.queue.updatedAtField, values.updatedAt],
      [this.config.airtable.queue.attemptCountField, values.attemptCount],
      [this.config.airtable.queue.heartbeatField, values.heartbeatAt],
      [this.config.airtable.queue.seededAtField, values.seededAt],
      [this.config.airtable.queue.completedAtField, values.completedAt]
    ];

    for (const [fieldName, value] of mappings) {
      if (!fieldName || value === undefined) {
        continue;
      }
      fields[fieldName] = value;
    }

    if (this.config.airtable.queue.adLinkField && values.adLinkRecordId !== undefined) {
      fields[this.config.airtable.queue.adLinkField] = values.adLinkRecordId ? [values.adLinkRecordId] : null;
    }

    if (this.config.airtable.queue.adLinkField && values.adLinkValue !== undefined) {
      fields[this.config.airtable.queue.adLinkField] = values.adLinkValue;
    }

    return fields;
  }

  buildAdsFields(values = {}) {
    const fields = {};
    if (this.config.airtable.ads.transcriptField && values.transcript !== undefined) {
      fields[this.config.airtable.ads.transcriptField] = values.transcript;
    }
    return fields;
  }

  async listSourceAds() {
    this.ensureConfigured();
    const records = await this.listRecords(this.config.airtable.ads.table, {
      view: this.config.airtable.ads.view
    });
    return records.map((record) => this.normalizeSourceAd(record));
  }

  async listQueueRecords() {
    this.ensureConfigured();
    const records = await this.listRecords(this.config.airtable.queue.table, {
      view: this.config.airtable.queue.view
    });
    return records.map((record) => this.normalizeQueueRecord(record));
  }

  async getQueueState({ retryLimit, staleMinutes }) {
    const records = await this.listQueueRecords();
    const staleBeforeMs = Date.now() - staleMinutes * 60 * 1000;
    let pending = 0;
    let running = 0;
    let done = 0;
    let failed = 0;
    let staleRunning = 0;
    let retryableFailed = 0;

    const visibleRecords = [...records].sort((left, right) => {
      const priorityDelta = claimPriority(left, staleBeforeMs) - claimPriority(right, staleBeforeMs);
      if (priorityDelta !== 0) {
        return priorityDelta;
      }
      return compareByUpdated(left, right);
    });

    for (const record of visibleRecords) {
      if (record.status === "done") {
        done += 1;
        continue;
      }

      if (record.status === "running") {
        running += 1;
        if (isStaleRunning(record, staleBeforeMs)) {
          staleRunning += 1;
        }
        continue;
      }

      if (record.status === "failed") {
        failed += 1;
        if (isRetryable(record, retryLimit)) {
          retryableFailed += 1;
        }
        continue;
      }

      pending += 1;
    }

    return {
      total: visibleRecords.length,
      pending,
      running,
      done,
      failed,
      staleRunning,
      retryableFailed,
      records: visibleRecords.slice(0, 200)
    };
  }

  async hasOutstandingWork({ retryLimit, staleMinutes }) {
    const state = await this.getQueueState({ retryLimit, staleMinutes });
    return state.pending > 0 || state.retryableFailed > 0 || state.staleRunning > 0;
  }

  async seedQueueFromAds() {
    const now = nowIso();
    const ads = await this.listSourceAds();
    const queueRecords = await this.listQueueRecords();
    const queueByAdsRecordId = new Map();
    const queueByAdId = new Map();
    for (const row of queueRecords) {
      const sourceRecordId = firstText(row.adsRecordId, row.linkedAdRecordId);
      if (sourceRecordId && !queueByAdsRecordId.has(sourceRecordId)) {
        queueByAdsRecordId.set(sourceRecordId, row);
      }
      if (row.adId && !queueByAdId.has(row.adId)) {
        queueByAdId.set(row.adId, row);
      }
    }

    let eligible = 0;
    let created = 0;
    let existing = 0;
    let updated = 0;
    let skippedMissingAdId = 0;
    let skippedMissingAttachment = 0;
    let skippedAlreadyTranscribed = 0;

    const creates = [];
    const updates = [];

    for (const ad of ads) {
      const needsImageReprocess = isImageLikeAttachment(ad.attachment) && isNoSpeechTranscript(ad.transcript);
      if (!ad.adId) {
        skippedMissingAdId += 1;
        continue;
      }
      if (!ad.attachment?.url) {
        skippedMissingAttachment += 1;
        continue;
      }
      if (ad.transcript && !needsImageReprocess) {
        skippedAlreadyTranscribed += 1;
        continue;
      }

      eligible += 1;
      const existingRow = queueByAdsRecordId.get(ad.recordId) || queueByAdId.get(ad.adId);
      if (!existingRow) {
        creates.push({
          fields: this.buildQueueFields({
            adId: ad.adId,
            adsRecordId: ad.recordId,
            adLinkValue:
              this.config.airtable.queue.adLinkField && !this.config.airtable.queue.adIdField && !this.config.airtable.queue.adsRecordIdField
                ? ad.adId
                : undefined,
            adLinkRecordId:
              this.config.airtable.queue.adLinkField && (this.config.airtable.queue.adIdField || this.config.airtable.queue.adsRecordIdField)
                ? ad.recordId
                : undefined,
            status: "pending",
            error: null,
            updatedAt: now,
            attemptCount: 0,
            heartbeatAt: null,
            seededAt: now,
            completedAt: null
          })
        });
        continue;
      }

      existing += 1;
      const fields = {};
      if (needsImageReprocess && existingRow.status === "done") {
        if (this.config.airtable.queue.transcriptField) {
          fields[this.config.airtable.queue.transcriptField] = null;
        }
        if (this.config.airtable.queue.statusField) {
          fields[this.config.airtable.queue.statusField] = "pending";
        }
        if (this.config.airtable.queue.errorField) {
          fields[this.config.airtable.queue.errorField] = null;
        }
        if (this.config.airtable.queue.heartbeatField) {
          fields[this.config.airtable.queue.heartbeatField] = null;
        }
        if (this.config.airtable.queue.completedAtField) {
          fields[this.config.airtable.queue.completedAtField] = null;
        }
      }
      if (this.config.airtable.queue.adsRecordIdField && !existingRow.adsRecordId) {
        fields[this.config.airtable.queue.adsRecordIdField] = ad.recordId;
      }
      if (this.config.airtable.queue.adLinkField && !existingRow.hasAdLink) {
        fields[this.config.airtable.queue.adLinkField] =
          !this.config.airtable.queue.adIdField && !this.config.airtable.queue.adsRecordIdField ? ad.adId : [ad.recordId];
      }
      if (this.config.airtable.queue.adIdField && !existingRow.adId) {
        fields[this.config.airtable.queue.adIdField] = ad.adId;
      }

      if (Object.keys(fields).length) {
        if (this.config.airtable.queue.updatedAtField) {
          fields[this.config.airtable.queue.updatedAtField] = now;
        }
        updates.push({ id: existingRow.recordId, fields });
      }
    }

    for (const batch of chunk(creates, MAX_BATCH)) {
      await this.requestJson("POST", this.config.airtable.queue.table, {
        body: {
          records: batch,
          typecast: true
        }
      });
      created += batch.length;
    }

    for (const batch of chunk(updates, MAX_BATCH)) {
      await this.requestJson("PATCH", this.config.airtable.queue.table, {
        body: {
          records: batch,
          typecast: true
        }
      });
      updated += batch.length;
    }

    return {
      eligible,
      created,
      existing,
      updated,
      skippedMissingAdId,
      skippedMissingAttachment,
      skippedAlreadyTranscribed
    };
  }

  async claimNextQueueRecord({ retryLimit, staleMinutes }) {
    const records = await this.listQueueRecords();
    const staleBeforeMs = Date.now() - staleMinutes * 60 * 1000;
    const candidates = records
      .filter((record) => isEligibleQueueRecord(record, { retryLimit, staleBeforeMs }))
      .sort((left, right) => {
        const priorityDelta = claimPriority(left, staleBeforeMs) - claimPriority(right, staleBeforeMs);
        if (priorityDelta !== 0) {
          return priorityDelta;
        }
        return compareByUpdated(left, right);
      });

    const candidate = candidates[0];
    if (!candidate) {
      return null;
    }

    const now = nowIso();
    const claimed = {
      ...candidate,
      status: "running",
      error: "",
      attemptCount: candidate.attemptCount + 1,
      updatedAt: now,
      heartbeatAt: now,
      heartbeatMs: Date.parse(now) || 0
    };

    await this.updateRecord(
      this.config.airtable.queue.table,
      candidate.recordId,
      this.buildQueueFields({
        status: "running",
        error: null,
        attemptCount: claimed.attemptCount,
        updatedAt: now,
        heartbeatAt: now
      })
    );

    return claimed;
  }

  async getSourceAdForQueueRecord(queueRecord) {
    const sourceRecordId = firstText(queueRecord.adsRecordId, queueRecord.linkedAdRecordId);
    if (sourceRecordId) {
      const record = await this.findOneRecord(
        this.config.airtable.ads.table,
        `RECORD_ID()='${escapeFormulaValue(sourceRecordId)}'`
      );
      if (record) {
        return this.normalizeSourceAd(record);
      }
    }

    if (!queueRecord.adId) {
      return null;
    }

    const record = await this.findOneRecord(
      this.config.airtable.ads.table,
      `{${this.config.airtable.ads.adIdField}}='${escapeFormulaValue(queueRecord.adId)}'`
    );

    return record ? this.normalizeSourceAd(record) : null;
  }

  async completeQueueRecord(recordId, transcript) {
    const now = nowIso();
    await this.updateRecord(
      this.config.airtable.queue.table,
      recordId,
      this.buildQueueFields({
        transcript,
        status: "done",
        error: null,
        updatedAt: now,
        heartbeatAt: now,
        completedAt: now
      })
    );
  }

  async writeTranscriptToAds(sourceAdRecordId, transcript) {
    await this.updateRecord(
      this.config.airtable.ads.table,
      sourceAdRecordId,
      this.buildAdsFields({
        transcript
      })
    );
  }

  async failQueueRecord(recordId, message) {
    const now = nowIso();
    await this.updateRecord(
      this.config.airtable.queue.table,
      recordId,
      this.buildQueueFields({
        transcript: null,
        status: "failed",
        error: excerpt(message, 100000),
        updatedAt: now,
        heartbeatAt: now
      })
    );
  }

  async retryFailedRecords() {
    const records = await this.listQueueRecords();
    const failed = records.filter((record) => record.status === "failed");
    if (!failed.length) {
      return { reset: 0 };
    }

    let reset = 0;
    for (const batch of chunk(failed, MAX_BATCH)) {
      await this.requestJson("PATCH", this.config.airtable.queue.table, {
        body: {
          records: batch.map((record) => ({
            id: record.recordId,
            fields: this.buildQueueFields({
              transcript: null,
              status: "pending",
              error: null,
              attemptCount: 0,
              heartbeatAt: null,
              updatedAt: nowIso(),
              completedAt: null
            })
          })),
          typecast: true
        }
      });
      reset += batch.length;
    }

    return { reset };
  }

  async listRecords(tableName, options = {}) {
    const records = [];
    let offset = "";

    while (true) {
      const query = { pageSize: 100 };
      if (options.view) {
        query.view = options.view;
      }
      if (options.filterByFormula) {
        query.filterByFormula = options.filterByFormula;
      }
      if (options.maxRecords) {
        query.maxRecords = options.maxRecords;
      }
      if (offset) {
        query.offset = offset;
      }

      const payload = await this.requestJson("GET", tableName, { query });
      if (Array.isArray(payload.records)) {
        records.push(...payload.records);
      }
      if (!payload.offset || (options.maxRecords && records.length >= options.maxRecords)) {
        break;
      }
      offset = payload.offset;
    }

    return records;
  }

  async findOneRecord(tableName, filterByFormula) {
    const records = await this.listRecords(tableName, {
      filterByFormula,
      maxRecords: 1
    });
    return records[0] || null;
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
