import { AirtableClient as AdplexityAirtableClient } from "./extractors/adplexity/airtable.js";
import { config as adplexityConfig } from "./extractors/adplexity/config.js";
import { runAdplexityChunk, seedAdplexityQueue } from "./extractors/adplexity/adplexity-runner.js";
import { createRuntime as createAdplexityRuntime } from "./extractors/adplexity/runtime.js";
import { AirtableClient as ForeplayAirtableClient } from "./extractors/foreplay/airtable.js";
import { config as foreplayConfig } from "./extractors/foreplay/config.js";
import { runForeplayChunk, seedForeplayQueue } from "./extractors/foreplay/foreplay-runner.js";
import { createRuntime as createForeplayRuntime } from "./extractors/foreplay/runtime.js";
import { AirtableClient as MetaAirtableClient } from "./extractors/meta/airtable.js";
import { config as metaConfig } from "./extractors/meta/config.js";
import { runMetaChunk, seedMetaQueue } from "./extractors/meta/meta-runner.js";
import { createRuntime as createMetaRuntime } from "./extractors/meta/runtime.js";

function createForeplayExtractor() {
  const descriptor = {
    id: "foreplay",
    label: "Foreplay",
    description: "Seeds creative-test winner checkpoints from Foreplay and writes winner ads to Airtable.",
    supportsClearCheckpoints: true
  };
  const runtime = createForeplayRuntime();
  const airtable = new ForeplayAirtableClient(foreplayConfig, runtime.log);

  return {
    ...descriptor,
    runtime,
    async getState() {
      const queue = await airtable.getCheckpointState({
        retryLimit: foreplayConfig.runtime.retryLimit,
        staleMinutes: foreplayConfig.runtime.staleMinutes
      });

      return {
        extractor: descriptor,
        config: {
          competitorsTable: foreplayConfig.airtable.competitorsTable,
          adsTable: foreplayConfig.airtable.adsTable,
          checkpointsTable: foreplayConfig.airtable.checkpointsTable,
          competitorsForeplayField: foreplayConfig.airtable.competitorsForeplayField,
          chunkMaxMs: foreplayConfig.runtime.chunkMaxMs,
          chunkMaxCheckpoints: foreplayConfig.runtime.chunkMaxCheckpoints,
          retryLimit: foreplayConfig.runtime.retryLimit,
          staleMinutes: foreplayConfig.runtime.staleMinutes
        },
        runtime: runtime.snapshot(),
        queue
      };
    },
    async seed(payload) {
      return seedForeplayQueue({
        airtable,
        config: foreplayConfig,
        runtime,
        payload
      });
    },
    async runChunk(payload) {
      return runForeplayChunk({
        airtable,
        config: foreplayConfig,
        runtime,
        payload
      });
    },
    requestStop() {
      runtime.requestStop();
    },
    async clearCheckpoints() {
      const summary = await airtable.clearCheckpoints();
      runtime.log(`Cleared ${summary.deleted} Foreplay checkpoints.`);
      return summary;
    }
  };
}

function createAdplexityExtractor() {
  const descriptor = {
    id: "adplexity",
    label: "AdPlexity",
    description: "Seeds AdPlexity report checkpoints and enriches ads into Airtable in resumable chunks.",
    supportsClearCheckpoints: true
  };
  const runtime = createAdplexityRuntime();
  const airtable = new AdplexityAirtableClient(adplexityConfig, runtime.log);

  return {
    ...descriptor,
    runtime,
    async getState() {
      const queue = await airtable.getCheckpointState({
        retryLimit: adplexityConfig.runtime.retryLimit,
        staleMinutes: adplexityConfig.runtime.staleMinutes
      });

      return {
        extractor: descriptor,
        config: {
          competitorsTable: adplexityConfig.airtable.competitorsTable,
          adsTable: adplexityConfig.airtable.adsTable,
          checkpointsTable: adplexityConfig.airtable.checkpointsTable,
          competitorsAdplexityField: adplexityConfig.airtable.competitorsAdplexityField,
          chunkMaxMs: adplexityConfig.runtime.chunkMaxMs,
          chunkMaxCheckpoints: adplexityConfig.runtime.chunkMaxCheckpoints,
          retryLimit: adplexityConfig.runtime.retryLimit,
          staleMinutes: adplexityConfig.runtime.staleMinutes
        },
        runtime: runtime.snapshot(),
        queue
      };
    },
    async seed(payload) {
      return seedAdplexityQueue({
        airtable,
        config: adplexityConfig,
        runtime,
        payload
      });
    },
    async runChunk(payload) {
      return runAdplexityChunk({
        airtable,
        config: adplexityConfig,
        runtime,
        payload
      });
    },
    requestStop() {
      runtime.requestStop();
    },
    async clearCheckpoints() {
      const summary = await airtable.clearCheckpoints();
      runtime.log(`Cleared ${summary.deleted} AdPlexity checkpoints.`);
      return summary;
    }
  };
}

function createMetaExtractor() {
  const descriptor = {
    id: "meta",
    label: "Meta",
    description: "Seeds Meta page scans and writes scraped Ad Library ads to Airtable with resumable checkpoints.",
    supportsClearCheckpoints: true
  };
  const runtime = createMetaRuntime();
  const airtable = new MetaAirtableClient(metaConfig, runtime.log);

  return {
    ...descriptor,
    runtime,
    async getState() {
      const queue = await airtable.getCheckpointState({
        retryLimit: metaConfig.runtime.retryLimit,
        staleMinutes: metaConfig.runtime.staleMinutes
      });

      return {
        extractor: descriptor,
        config: {
          competitorsTable: metaConfig.airtable.competitorsTable,
          adsTable: metaConfig.airtable.adsTable,
          checkpointsTable: metaConfig.airtable.checkpointsTable,
          competitorsMetaPageField: metaConfig.airtable.competitorsMetaPageField,
          chunkMaxMs: metaConfig.runtime.chunkMaxMs,
          chunkMaxCheckpoints: metaConfig.runtime.chunkMaxCheckpoints,
          retryLimit: metaConfig.runtime.retryLimit,
          staleMinutes: metaConfig.runtime.staleMinutes,
          minDays: metaConfig.runtime.minDays,
          media: metaConfig.runtime.media,
          maxAdsPerPage: metaConfig.runtime.maxAdsPerPage,
          sliceMaxAds: metaConfig.runtime.sliceMaxAds,
          metaFormTemplateConfigured: Boolean(metaConfig.metaApi.formTemplate),
          metaCookieConfigured: Boolean(metaConfig.metaApi.cookie)
        },
        runtime: runtime.snapshot(),
        queue
      };
    },
    async seed(payload) {
      return seedMetaQueue({
        airtable,
        config: metaConfig,
        runtime,
        payload
      });
    },
    async runChunk(payload) {
      return runMetaChunk({
        airtable,
        config: metaConfig,
        runtime,
        payload
      });
    },
    requestStop() {
      runtime.requestStop();
    },
    async clearCheckpoints() {
      const summary = await airtable.clearCheckpoints();
      runtime.log(`Cleared ${summary.deleted} Meta checkpoints.`);
      return summary;
    }
  };
}

const extractorMap = {
  foreplay: createForeplayExtractor(),
  adplexity: createAdplexityExtractor(),
  meta: createMetaExtractor()
};

const extractorIds = ["foreplay", "adplexity", "meta"];

export function listExtractors() {
  return extractorIds.map((id) => {
    const extractor = extractorMap[id];
    return {
      id: extractor.id,
      label: extractor.label,
      description: extractor.description,
      supportsClearCheckpoints: extractor.supportsClearCheckpoints
    };
  });
}

export function getExtractor(id) {
  return extractorMap[id] || null;
}
