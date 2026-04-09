const runtimeTitle = document.querySelector("#runtime-title");
const runtimeBadge = document.querySelector("#runtime-badge");
const configNote = document.querySelector("#config-note");
const seedButton = document.querySelector("#seed-btn");
const runButton = document.querySelector("#run-btn");
const stopButton = document.querySelector("#stop-btn");
const refreshButton = document.querySelector("#refresh-btn");
const retryButton = document.querySelector("#retry-btn");
const logsEl = document.querySelector("#logs");
const queueJsonEl = document.querySelector("#queue-json");
const rowsBody = document.querySelector("#rows-body");
const rowCount = document.querySelector("#row-count");
const browserGemini = window.__APP_CONFIG__?.browserGemini || {};
const DEFAULT_TRANSCRIPTION_PROMPT =
  "Transcribe all spoken words from this video verbatim from start to finish. Do not summarize, shorten, paraphrase, or describe the video. Return only the complete transcript text in reading order. If there is no speech, return exactly NO_SPEECH.";
const DEFAULT_IMAGE_DESCRIPTION_PROMPT =
  "This file is a static image ad, not a video. Do not return NO_SPEECH. Describe this ad image in one short paragraph for marketing classification. Mention the core visual, product or offer, any visible text or CTA, audience cues, and the likely creative angle. Keep it concise, plain, and useful for later vertical filtering. Return only the paragraph.";

const uiState = {
  autoRun: false,
  lastError: "",
  latestState: null,
  pending: {
    seed: false,
    run: false,
    stop: false,
    refresh: false,
    retry: false
  }
};

function text(selector, value) {
  const node = document.querySelector(selector);
  if (node) {
    node.textContent = String(value ?? "");
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function statusPill(status) {
  const normalized = String(status || "").toLowerCase() || "pending";
  return `<span class="pill pill-${escapeHtml(normalized)}">${escapeHtml(normalized)}</span>`;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || "GET",
    headers: {
      "content-type": "application/json"
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return payload;
}

function inferMimeType(attachment, options = {}) {
  const responseType = String(options.responseType || "").trim().toLowerCase();
  const type = String(attachment?.type || "").trim().toLowerCase();
  if (type) {
    if (type.startsWith("image/") || type.startsWith("video/")) {
      return type;
    }
  }

  if (responseType) {
    if (responseType.startsWith("image/") || responseType.startsWith("video/")) {
      return responseType;
    }
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

  const bytes = options.bytes instanceof Uint8Array ? options.bytes : null;
  if (bytes && bytes.length >= 12) {
    if (bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47) {
      return "image/png";
    }
    if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) {
      return "image/jpeg";
    }
    if (
      bytes[0] === 0x47 &&
      bytes[1] === 0x49 &&
      bytes[2] === 0x46 &&
      bytes[3] === 0x38
    ) {
      return "image/gif";
    }
    if (
      bytes[0] === 0x52 &&
      bytes[1] === 0x49 &&
      bytes[2] === 0x46 &&
      bytes[3] === 0x46 &&
      bytes[8] === 0x57 &&
      bytes[9] === 0x45 &&
      bytes[10] === 0x42 &&
      bytes[11] === 0x50
    ) {
      return "image/webp";
    }
    if (
      bytes[4] === 0x66 &&
      bytes[5] === 0x74 &&
      bytes[6] === 0x79 &&
      bytes[7] === 0x70
    ) {
      const brand = String.fromCharCode(...bytes.slice(8, 12)).toLowerCase();
      if (brand.startsWith("qt")) {
        return "video/quicktime";
      }
      return "video/mp4";
    }
    if (bytes[0] === 0x1a && bytes[1] === 0x45 && bytes[2] === 0xdf && bytes[3] === 0xa3) {
      return "video/webm";
    }
  }

  return "application/octet-stream";
}

function classifyAttachment(attachment) {
  const mimeType = inferMimeType(attachment);
  return {
    mimeType,
    isImage: mimeType.startsWith("image/") || !mimeType.startsWith("video/"),
    isVideo: mimeType.startsWith("video/")
  };
}

function toBase64(arrayBuffer) {
  const bytes = new Uint8Array(arrayBuffer);
  const chunkSize = 0x8000;
  let binary = "";

  for (let index = 0; index < bytes.length; index += chunkSize) {
    const slice = bytes.subarray(index, index + chunkSize);
    binary += String.fromCharCode(...slice);
  }

  return btoa(binary);
}

function extractGeminiText(payload) {
  const directText = String(payload?.text || "").trim();
  if (directText) {
    return directText;
  }

  const candidates = Array.isArray(payload?.candidates) ? payload.candidates : [];
  const parts = candidates.flatMap((candidate) =>
    Array.isArray(candidate?.content?.parts) ? candidate.content.parts : []
  );
  const text = parts
    .map((part) => String(part?.text || "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();

  if (text) {
    return text;
  }

  const blockReason = String(payload?.promptFeedback?.blockReason || "").trim();
  if (blockReason) {
    throw new Error(`Gemini blocked the request: ${blockReason}`);
  }

  throw new Error("Gemini returned no transcript text.");
}

function isEnvironmentFailure(message) {
  const text = String(message || "");
  return (
    text.includes("API_KEY_INVALID") ||
    text.includes("API key not valid") ||
    text.includes("Missing Gemini API key") ||
    text.includes("Missing required environment variable") ||
    text.includes("Gemini API key")
  );
}

async function transcribeTaskInBrowser(task) {
  if (!browserGemini.apiKey) {
    throw new Error("Missing Gemini API key in browser runtime.");
  }

  if (!task?.attachment?.url) {
    throw new Error("Attachment URL is missing.");
  }

  if (task.attachment.size && task.attachment.size > Number(browserGemini.maxInlineBytes || 0)) {
    throw new Error(
      `Attachment is ${task.attachment.size} bytes and exceeds the ${browserGemini.maxInlineBytes} byte inline limit.`
    );
  }

  const download = await fetch(task.attachment.url, {
    signal: AbortSignal.timeout(Number(browserGemini.timeoutMs || 120000))
  });
  if (!download.ok) {
    throw new Error(`Attachment download failed (${download.status}).`);
  }

  const arrayBuffer = await download.arrayBuffer();
  const bytes = new Uint8Array(arrayBuffer);
  if (!arrayBuffer.byteLength) {
    throw new Error("Attachment download returned an empty file.");
  }
  if (browserGemini.maxInlineBytes && arrayBuffer.byteLength > Number(browserGemini.maxInlineBytes)) {
    throw new Error(
      `Attachment is ${arrayBuffer.byteLength} bytes and exceeds the ${browserGemini.maxInlineBytes} byte inline limit.`
    );
  }

  const responseType = String(download.headers.get("content-type") || "")
    .split(";")[0]
    .trim()
    .toLowerCase();
  const mimeType = inferMimeType(task.attachment, { responseType, bytes });
  const attachmentKind = {
    mimeType,
    isImage: mimeType.startsWith("image/") || !mimeType.startsWith("video/"),
    isVideo: mimeType.startsWith("video/")
  };
  const language = String(task.language || browserGemini.language || "").trim();
  const basePrompt = attachmentKind.isImage
    ? browserGemini.imagePrompt || DEFAULT_IMAGE_DESCRIPTION_PROMPT
    : browserGemini.prompt || DEFAULT_TRANSCRIPTION_PROMPT;
  const prompt =
    language && !attachmentKind.isImage
      ? `${basePrompt}\n\nLanguage hint: ${language}.`
      : basePrompt;
  const selectedModel = attachmentKind.isImage
    ? browserGemini.imageModel || "gemini-2.0-flash"
    : browserGemini.model || "gemini-2.5-flash";

  const response = await fetch(
    `${String(browserGemini.apiUrl || "https://generativelanguage.googleapis.com/v1beta").replace(/\/$/, "")}/models/${encodeURIComponent(selectedModel)}:generateContent`,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-goog-api-key": browserGemini.apiKey
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              { text: prompt },
              {
                inline_data: {
                  mime_type: attachmentKind.mimeType,
                  data: toBase64(arrayBuffer)
                }
              }
            ]
          }
        ],
        generationConfig: {
          responseMimeType: "text/plain",
          maxOutputTokens: Number(browserGemini.maxOutputTokens || 8192),
          temperature: Number(browserGemini.temperature ?? 0.1)
        }
      }),
      signal: AbortSignal.timeout(Number(browserGemini.timeoutMs || 120000))
    }
  );

  if (!response.ok) {
    const message = (await response.text().catch(() => "")).slice(0, 1000);
    throw new Error(`Gemini request failed: ${message}`);
  }

  const payload = await response.json();

  return {
    text: extractGeminiText(payload),
    bytes: arrayBuffer.byteLength
  };
}

function setPending(key, value) {
  uiState.pending[key] = value;
  render();
}

function buildConfigNote(state) {
  if (!state) {
    return "State unavailable until Airtable and Gemini are configured.";
  }

  const bits = [];
  if (!state.config.airtableConfigured) {
    bits.push("Missing Airtable credentials in .env.");
  }
  if (!state.config.geminiConfigured) {
    bits.push("Missing Gemini API key in runtime secrets or .env.");
  } else if (state.config.geminiApiKeyName) {
    bits.push(
      `Gemini auth: ${state.config.geminiApiKeyName} from ${state.config.geminiApiKeySource || "runtime secret"}.`
    );
  }

  bits.push(
    `Seed source: ${state.config.adsTable}${state.config.adsView ? ` (view: ${state.config.adsView})` : ""}. Attachment field: ${state.config.adsAttachmentField}.`
  );
  bits.push(
    `Queue table: ${state.config.queueTable}${state.config.queueView ? ` (view: ${state.config.queueView})` : ""}. ${state.config.queueAdIdField ? `Ad Id field: ${state.config.queueAdIdField}.` : "Ad Id field disabled; linked Ad record is used instead."}`
  );
  bits.push(`Queue transcript field: ${state.config.queueTranscriptField || "disabled"}.`);
  bits.push(
    `Output: writes transcript back to ${state.config.adsTable}.${state.config.adsTranscriptField}.`
  );
  bits.push(
    `Model: ${state.config.model}${state.config.language ? `, language ${state.config.language}` : ""}. Inline limit: ${state.config.maxInlineBytes} bytes. Gemini calls run directly from the browser. Images get marketer-style visual descriptions.`
  );
  bits.push(
    `Chunk budget: ${Math.round(state.config.chunkMaxMs / 1000)}s or ${state.config.chunkMaxRecords} rows. Retry limit: ${state.config.retryLimit}.`
  );

  if (state.config.queueAdLinkField) {
    bits.push(`Seed also fills linked record field: ${state.config.queueAdLinkField}.`);
  }

  return bits.join(" ");
}

function renderRows(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    rowsBody.innerHTML = `<tr><td colspan="7" class="empty">No queue rows yet.</td></tr>`;
    rowCount.textContent = "0 rows";
    return;
  }

  rowsBody.innerHTML = rows
    .map((row) => {
      const updated = row.updatedAt ? new Date(row.updatedAt).toLocaleString() : "-";
      const errorText = String(row.error || "").slice(0, 160);
      return `
        <tr>
          <td>${statusPill(row.status)}</td>
          <td class="mono">${escapeHtml(row.recordId)}</td>
          <td class="mono">${escapeHtml(row.adId || "")}</td>
          <td class="mono">${escapeHtml(row.adsRecordId || row.linkedAdRecordId || "")}</td>
          <td>${escapeHtml(row.attemptCount ?? 0)}</td>
          <td>${escapeHtml(updated)}</td>
          <td title="${escapeHtml(row.error || "")}">${escapeHtml(errorText)}</td>
        </tr>
      `;
    })
    .join("");

  rowCount.textContent = `${rows.length} rows shown`;
}

function renderButtons() {
  const state = uiState.latestState;
  const isRunning = uiState.autoRun || state?.runtime?.running;
  const anyPending = Object.values(uiState.pending).some(Boolean);

  seedButton.disabled = Boolean(isRunning || uiState.pending.seed || uiState.pending.retry);
  runButton.disabled = Boolean(isRunning || uiState.pending.run || uiState.pending.seed || uiState.pending.retry);
  stopButton.disabled = Boolean(!isRunning || uiState.pending.stop);
  refreshButton.disabled = Boolean(uiState.pending.refresh);
  retryButton.disabled = Boolean(anyPending || isRunning);
}

function render() {
  const state = uiState.latestState;
  renderButtons();

  if (!state) {
    runtimeTitle.textContent = uiState.lastError ? "Unavailable" : "Loading...";
    runtimeBadge.textContent = uiState.lastError || "Waiting for state...";
    configNote.textContent = "State unavailable until Airtable and Gemini are configured.";
    logsEl.textContent = uiState.lastError || "Waiting for state...";
    queueJsonEl.textContent = uiState.lastError || "Waiting for queue state...";
    renderRows([]);
    return;
  }

  runtimeTitle.textContent = state.runtime.running ? "Running" : uiState.autoRun ? "Auto-running" : "Idle";
  runtimeBadge.textContent = state.runtime.running
    ? `Processing ${state.runtime.currentCheckpointKey || "queue row"}`
    : uiState.autoRun
      ? "Continuing queue"
      : "Waiting";

  configNote.textContent = buildConfigNote(state);
  logsEl.textContent =
    Array.isArray(state.runtime.logs) && state.runtime.logs.length ? state.runtime.logs.join("\n") : "No logs yet.";

  text("#total-count", state.queue.total ?? 0);
  text("#pending-count", state.queue.pending ?? 0);
  text("#running-count", state.queue.running ?? 0);
  text("#done-count", state.queue.done ?? 0);
  text("#failed-count", state.queue.failed ?? 0);
  text("#retryable-count", state.queue.retryableFailed ?? 0);
  text("#current-record", state.runtime.currentCheckpointKey || "None");
  text("#generated-at", state.runtime.generatedAt ? new Date(state.runtime.generatedAt).toLocaleString() : "-");

  queueJsonEl.textContent = JSON.stringify(
    {
      total: state.queue.total ?? 0,
      pending: state.queue.pending ?? 0,
      running: state.queue.running ?? 0,
      done: state.queue.done ?? 0,
      failed: state.queue.failed ?? 0,
      staleRunning: state.queue.staleRunning ?? 0,
      retryableFailed: state.queue.retryableFailed ?? 0
    },
    null,
    2
  );

  renderRows(state.queue.records || []);
}

function storeState(state) {
  uiState.latestState = state;
  uiState.lastError = "";
  render();
}

async function refreshState({ silent = false } = {}) {
  if (!silent) {
    setPending("refresh", true);
  }

  try {
    const payload = await request("/api/state");
    storeState(payload);
    return payload;
  } catch (error) {
    uiState.lastError = error.message;
    render();
    throw error;
  } finally {
    if (!silent) {
      setPending("refresh", false);
    }
  }
}

async function seedQueue() {
  setPending("seed", true);
  try {
    const payload = await request("/api/seed", { method: "POST" });
    storeState(payload.state);
    return payload;
  } finally {
    setPending("seed", false);
  }
}

async function runLoop() {
  if (uiState.autoRun) {
    return;
  }

  uiState.autoRun = true;
  setPending("run", true);
  let processed = 0;
  let done = 0;
  let failed = 0;
  let reason = "queue-empty";

  try {
    while (uiState.autoRun) {
      const payload = await request("/api/claim-browser-task", { method: "POST" });
      storeState(payload.state);

      if (!payload.task) {
        reason = payload.reason || (uiState.latestState?.runtime?.stopRequested ? "stop-requested" : "queue-empty");
        break;
      }

      processed += 1;

      try {
        const result = await transcribeTaskInBrowser(payload.task);
        const completePayload = await request("/api/complete-browser-task", {
          method: "POST",
          body: {
            recordId: payload.task.recordId,
            sourceAdRecordId: payload.task.sourceAdRecordId,
            transcript: result.text,
            bytes: result.bytes
          }
        });
        storeState(completePayload.state);
        done += 1;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        failed += 1;
        const fatal = isEnvironmentFailure(message);
        const failPayload = await request("/api/fail-browser-task", {
          method: "POST",
          body: {
            recordId: payload.task.recordId,
            key: payload.task.key,
            error: message,
            fatal
          }
        });
        storeState(failPayload.state);
        if (fatal) {
          reason = "environment-error";
          uiState.autoRun = false;
          break;
        }
      }

      if (uiState.latestState?.runtime?.stopRequested) {
        reason = "stop-requested";
        break;
      }

      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  } finally {
    const finishPayload = await request("/api/finish-browser-run", {
      method: "POST",
      body: {
        processed,
        done,
        failed,
        reason
      }
    }).catch(() => null);
    if (finishPayload?.state) {
      storeState(finishPayload.state);
    }
    uiState.autoRun = false;
    setPending("run", false);
    render();
  }
}

async function stopRun() {
  uiState.autoRun = false;
  setPending("stop", true);
  try {
    const payload = await request("/api/stop", { method: "POST" });
    storeState(payload.state);
  } finally {
    setPending("stop", false);
  }
}

async function retryFailed() {
  setPending("retry", true);
  try {
    const payload = await request("/api/retry-failed", { method: "POST" });
    storeState(payload.state);
  } finally {
    setPending("retry", false);
  }
}

function handleActionError(error) {
  alert(error.message);
}

seedButton.addEventListener("click", () => {
  seedQueue().catch(handleActionError);
});

runButton.addEventListener("click", () => {
  runLoop().catch(handleActionError);
});

stopButton.addEventListener("click", () => {
  stopRun().catch(handleActionError);
});

refreshButton.addEventListener("click", () => {
  refreshState().catch(handleActionError);
});

retryButton.addEventListener("click", () => {
  retryFailed().catch(handleActionError);
});

setInterval(() => {
  if (document.hidden) {
    return;
  }

  refreshState({ silent: true }).catch(() => undefined);
}, 5000);

refreshState().catch((error) => {
  logsEl.textContent = error.message;
  queueJsonEl.textContent = error.message;
});
