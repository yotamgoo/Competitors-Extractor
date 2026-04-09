import { AirtableClient } from "./airtable.js";
import { config } from "./config.js";
import { GeminiClient } from "./gemini-client.js";
import { createRuntime } from "./runtime.js";

function inferAttachmentMimeType(attachment) {
  const type = String(attachment?.type || "").trim().toLowerCase();
  if (type.startsWith("image/") || type.startsWith("video/")) {
    return type;
  }

  const filename = String(attachment?.filename || "").trim().toLowerCase();
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

  const url = String(attachment?.url || "").trim().toLowerCase();
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
  return String(value || "").trim().toUpperCase() === "NO_SPEECH";
}

function shouldReuseExistingTranscript(sourceAd) {
  const transcript = String(sourceAd?.transcript || "").trim();
  if (!transcript) {
    return false;
  }

  if (isImageLikeAttachment(sourceAd?.attachment) && isNoSpeechTranscript(transcript)) {
    return false;
  }

  return true;
}

function isEnvironmentFailure(message) {
  const text = String(message || "");
  return (
    text.includes("API_KEY_INVALID") ||
    text.includes("API key not valid") ||
    text.includes("Missing required environment variable") ||
    text.includes("Gemini API key")
  );
}

export function createTranscriptionApp() {
  const runtime = createRuntime();
  const airtable = new AirtableClient(config, runtime.log);
  const gemini = new GeminiClient(config);

  return {
    runtime,
    requestStop() {
      runtime.requestStop();
    },
    async getState() {
      const queue = await airtable.getQueueState({
        retryLimit: config.runtime.retryLimit,
        staleMinutes: config.runtime.staleMinutes
      });

      return {
        config: {
          adsTable: config.airtable.ads.table,
          adsView: config.airtable.ads.view,
          adsAttachmentField: config.airtable.ads.attachmentField,
          adsTranscriptField: config.airtable.ads.transcriptField,
          queueTable: config.airtable.queue.table,
          queueView: config.airtable.queue.view,
          queueAdIdField: config.airtable.queue.adIdField,
          queueAdsRecordIdField: config.airtable.queue.adsRecordIdField,
          queueAdLinkField: config.airtable.queue.adLinkField,
          queueTranscriptField: config.airtable.queue.transcriptField,
          queueStatusField: config.airtable.queue.statusField,
          queueErrorField: config.airtable.queue.errorField,
          queueUpdatedAtField: config.airtable.queue.updatedAtField,
          queueAttemptCountField: config.airtable.queue.attemptCountField,
          queueHeartbeatField: config.airtable.queue.heartbeatField,
          chunkMaxMs: config.runtime.chunkMaxMs,
          chunkMaxRecords: config.runtime.chunkMaxRecords,
          retryLimit: config.runtime.retryLimit,
          maxInlineBytes: config.runtime.maxInlineBytes,
          model: config.gemini.model,
          language: config.gemini.language,
          airtableConfigured: Boolean(config.airtable.token && config.airtable.baseId),
          geminiConfigured: Boolean(config.gemini.apiKey),
          geminiApiKeyName: config.gemini.apiKeyName,
          geminiApiKeySource: config.gemini.apiKeySource
        },
        runtime: runtime.snapshot(),
        queue
      };
    },
    async claimBrowserTask() {
      if (!runtime.state.running) {
        runtime.begin("run-chunk");
      }

      while (!runtime.state.stopRequested) {
        const item = await airtable.claimNextQueueRecord({
          retryLimit: config.runtime.retryLimit,
          staleMinutes: config.runtime.staleMinutes
        });
        if (!item) {
          return {
            task: null,
            reason: "queue-empty",
            state: await this.getState()
          };
        }

        runtime.setCheckpoint(item.key);
        runtime.log(`Claimed ${item.key} (${item.label}).`);

        try {
          const sourceAd = await airtable.getSourceAdForQueueRecord(item);
          if (!sourceAd) {
            throw new Error(
              `Could not find matching Ads row for queue row ${item.recordId} (${item.adId || item.linkedAdRecordId || "no ad reference"}).`
            );
          }

          if (shouldReuseExistingTranscript(sourceAd)) {
            await airtable.completeQueueRecord(item.recordId, sourceAd.transcript);
            runtime.log(`Skipped Gemini for ${item.key}; Ads already has a transcript.`);
            continue;
          }
          if (isImageLikeAttachment(sourceAd.attachment) && isNoSpeechTranscript(sourceAd.transcript)) {
            runtime.log(`Reprocessing ${item.key}; image ad had stale NO_SPEECH output.`);
          }

          if (!sourceAd.attachment?.url) {
            throw new Error(
              `Matching Ads row for ${item.adId || sourceAd.recordId} has no ${config.airtable.ads.attachmentField} attachment.`
            );
          }

          return {
            task: {
              recordId: item.recordId,
              key: item.key,
              label: item.label,
              adId: item.adId,
              sourceAdRecordId: sourceAd.recordId,
              attachment: sourceAd.attachment,
              language: item.language
            },
            reason: "",
            state: await this.getState()
          };
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          await airtable.failQueueRecord(item.recordId, message);
          runtime.log(`Queue row ${item.key} failed: ${message}`);
          if (isEnvironmentFailure(message)) {
            runtime.requestStop();
            runtime.log(
              "Stopping after environment/config failure so retries are not burned unnecessarily."
            );
            return {
              task: null,
              reason: "environment-error",
              state: await this.getState()
            };
          }
        }
      }

      return {
        task: null,
        reason: "stop-requested",
        state: await this.getState()
      };
    },
    async completeBrowserTask({ recordId, sourceAdRecordId, transcript, bytes = 0 }) {
      await airtable.writeTranscriptToAds(sourceAdRecordId, transcript);
      await airtable.completeQueueRecord(recordId, transcript);
      runtime.log(
        `Transcribed queue:${recordId}: ${bytes} bytes, ${String(transcript || "").length} chars written back to Ads.`
      );
      return {
        ok: true,
        state: await this.getState()
      };
    },
    async failBrowserTask({ recordId, key = "", error = "", fatal = false }) {
      const message = error instanceof Error ? error.message : String(error);
      await airtable.failQueueRecord(recordId, message);
      runtime.log(`Queue row ${key || `queue:${recordId}`} failed: ${message}`);
      if (fatal || isEnvironmentFailure(message)) {
        runtime.requestStop();
        runtime.log("Stopping after environment/config failure so retries are not burned unnecessarily.");
      }
      return {
        ok: true,
        state: await this.getState()
      };
    },
    async finishBrowserRun({ processed = 0, done = 0, failed = 0, reason = "queue-empty" } = {}) {
      runtime.log(
        `Chunk finished: processed ${processed}, done ${done}, failed ${failed}, reason=${reason}.`
      );
      if (runtime.state.running) {
        runtime.finish("run-chunk");
      }
      return {
        ok: true,
        state: await this.getState()
      };
    },
    async seed() {
      const summary = await airtable.seedQueueFromAds();
      runtime.log(
        `Seed complete: eligible ${summary.eligible}, created ${summary.created}, existing ${summary.existing}, updated ${summary.updated}, skipped-missing-ad-id ${summary.skippedMissingAdId}, skipped-missing-attachment ${summary.skippedMissingAttachment}, skipped-already-transcribed ${summary.skippedAlreadyTranscribed}.`
      );
      return summary;
    },
    async retryFailed() {
      const summary = await airtable.retryFailedRecords();
      runtime.log(`Reset ${summary.reset} failed queue rows back to pending.`);
      return summary;
    },
    async runChunk() {
      const startedAt = Date.now();
      let processed = 0;
      let done = 0;
      let failed = 0;
      let reasonOverride = "";

      runtime.begin("run-chunk");

      try {
        while (!runtime.state.stopRequested) {
          const elapsedMs = Date.now() - startedAt;
          if (elapsedMs >= config.runtime.chunkMaxMs) {
            break;
          }
          if (processed >= config.runtime.chunkMaxRecords) {
            break;
          }

          const item = await airtable.claimNextQueueRecord({
            retryLimit: config.runtime.retryLimit,
            staleMinutes: config.runtime.staleMinutes
          });
          if (!item) {
            break;
          }

          processed += 1;
          runtime.setCheckpoint(item.key);
          runtime.log(`Claimed ${item.key} (${item.label}).`);

          try {
            const sourceAd = await airtable.getSourceAdForQueueRecord(item);
            if (!sourceAd) {
              throw new Error(
                `Could not find matching Ads row for queue row ${item.recordId} (${item.adId || item.linkedAdRecordId || "no ad reference"}).`
              );
            }

            if (shouldReuseExistingTranscript(sourceAd)) {
              await airtable.completeQueueRecord(item.recordId, sourceAd.transcript);
              done += 1;
              runtime.log(`Skipped Gemini for ${item.key}; Ads already has a transcript.`);
              continue;
            }
            if (isImageLikeAttachment(sourceAd.attachment) && isNoSpeechTranscript(sourceAd.transcript)) {
              runtime.log(`Reprocessing ${item.key}; image ad had stale NO_SPEECH output.`);
            }

            if (!sourceAd.attachment?.url) {
              throw new Error(
                `Matching Ads row for ${item.adId || sourceAd.recordId} has no ${config.airtable.ads.attachmentField} attachment.`
              );
            }

            const result = await gemini.transcribeAttachment(sourceAd.attachment, {
              language: item.language
            });
            await airtable.writeTranscriptToAds(sourceAd.recordId, result.text);
            await airtable.completeQueueRecord(item.recordId, result.text);
            done += 1;
            runtime.log(
              `Transcribed ${item.key}: ${result.bytes} bytes, ${result.text.length} chars written back to Ads.`
            );
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            failed += 1;
            await airtable.failQueueRecord(item.recordId, message);
            runtime.log(`Queue row ${item.key} failed: ${message}`);
            if (isEnvironmentFailure(message)) {
              reasonOverride = "environment-error";
              runtime.requestStop();
              runtime.log(
                "Stopping after environment/config failure so retries are not burned unnecessarily."
              );
            }
          }
        }

        const queueStillHasWork =
          !runtime.state.stopRequested &&
          (await airtable.hasOutstandingWork({
            retryLimit: config.runtime.retryLimit,
            staleMinutes: config.runtime.staleMinutes
          }));

        const elapsedMs = Date.now() - startedAt;
        let reason = "queue-empty";
        if (reasonOverride) {
          reason = reasonOverride;
        } else if (runtime.state.stopRequested) {
          reason = "stop-requested";
        } else if (queueStillHasWork && elapsedMs >= config.runtime.chunkMaxMs) {
          reason = "budget-reached";
        } else if (queueStillHasWork && processed >= config.runtime.chunkMaxRecords) {
          reason = "row-limit-reached";
        }

        runtime.log(
          `Chunk finished: processed ${processed}, done ${done}, failed ${failed}, reason=${reason}.`
        );

        return {
          processed,
          done,
          failed,
          continueSuggested: queueStillHasWork && !runtime.state.stopRequested,
          reason
        };
      } finally {
        runtime.finish("run-chunk");
      }
    }
  };
}
