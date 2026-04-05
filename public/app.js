const tabList = document.querySelector("#tab-list");
const runtimeBadge = document.querySelector("#runtime-badge");
const activeExtractorLabel = document.querySelector("#active-extractor-label");
const extractorDescription = document.querySelector("#extractor-description");
const configNote = document.querySelector("#config-note");
const seedButton = document.querySelector("#seed-btn");
const runButton = document.querySelector("#run-btn");
const stopButton = document.querySelector("#stop-btn");
const refreshButton = document.querySelector("#refresh-btn");
const clearButton = document.querySelector("#clear-btn");
const logsEl = document.querySelector("#logs");
const queueJsonEl = document.querySelector("#queue-json");
const checkpointsHead = document.querySelector("#checkpoints-head");
const checkpointsBody = document.querySelector("#checkpoints-body");
const checkpointCount = document.querySelector("#checkpoint-count");

const forms = {
  foreplay: {
    months: document.querySelector("#foreplay-months"),
    manualBrandIds: document.querySelector("#foreplay-manual-brand-ids")
  },
  adplexity: {
    manualReportIds: document.querySelector("#adplexity-manual-report-ids")
  },
  meta: {
    manualPageIds: document.querySelector("#meta-manual-page-ids"),
    searchQuery: document.querySelector("#meta-search-query"),
    minDays: document.querySelector("#meta-min-days"),
    media: document.querySelector("#meta-media"),
    maxAdsPerPage: document.querySelector("#meta-max-ads-per-page")
  }
};

const sections = Array.from(document.querySelectorAll(".extractor-controls"));

const extractorMeta = {
  foreplay: {
    emptyRowText: "No Foreplay checkpoints yet."
  },
  adplexity: {
    emptyRowText: "No AdPlexity checkpoints yet."
  },
  meta: {
    emptyRowText: "No Meta checkpoints yet."
  }
};

const checkpointColumns = {
  foreplay: [
    { label: "Key", render: (item) => cell(item.key, "mono") },
    { label: "Status", render: (item) => statusPill(item.status) },
    { label: "Brand", render: (item) => cell(item.brandName || item.brandId || "") },
    { label: "Date", render: (item) => cell(item.bucketDate || "") },
    { label: "Attempts", render: (item) => cell(item.attemptCount ?? 0) },
    { label: "Ads", render: (item) => cell(item.adsWritten ?? 0) },
    { label: "Last Error", render: (item) => errorCell(item.lastError) }
  ],
  adplexity: [
    { label: "Key", render: (item) => cell(item.key, "mono") },
    { label: "Stage", render: (item) => cell(item.stage || "") },
    { label: "Status", render: (item) => statusPill(item.status) },
    { label: "Report", render: (item) => cell(item.reportName || item.reportId || "") },
    { label: "Cursor", render: (item) => cell(item.cursor || "") },
    { label: "Ad Id", render: (item) => cell(item.adId || "", "mono") },
    { label: "Attempts", render: (item) => cell(item.attemptCount ?? 0) },
    { label: "Ads", render: (item) => cell(item.adsWritten ?? 0) },
    { label: "Last Error", render: (item) => errorCell(item.lastError) }
  ],
  meta: [
    { label: "Key", render: (item) => cell(item.key, "mono") },
    { label: "Stage", render: (item) => cell(item.stage || "") },
    { label: "Status", render: (item) => statusPill(item.status) },
    { label: "Page", render: (item) => cell(item.pageName || item.pageId || "") },
    { label: "Search Query", render: (item) => cell(item.searchQuery || "") },
    { label: "Position", render: (item) => cell(item.position ?? 0) },
    { label: "Cursor", render: (item) => cell(item.cursor || "") },
    { label: "Last Library Id", render: (item) => cell(item.lastLibraryId || "", "mono") },
    { label: "Ads", render: (item) => cell(item.adsWritten ?? 0) },
    { label: "Last Error", render: (item) => errorCell(item.lastError) }
  ]
};

const uiState = {};
let extractorItems = [];
let activeExtractorId = "foreplay";

function getUiState(id) {
  if (!uiState[id]) {
    uiState[id] = {
      autoRun: false,
      lastError: "",
      latestState: null,
      pending: {
        seed: false,
        run: false,
        stop: false,
        refresh: false,
        clear: false
      }
    };
  }
  return uiState[id];
}

function text(id, value) {
  const node = document.querySelector(id);
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

function cell(value, className = "") {
  const content = escapeHtml(value);
  return `<td${className ? ` class="${className}"` : ""}>${content}</td>`;
}

function errorCell(value) {
  const textValue = String(value ?? "").trim();
  const shortText = textValue.slice(0, 140);
  return `<td title="${escapeHtml(textValue)}">${escapeHtml(shortText)}</td>`;
}

function statusPill(status) {
  const normalized = String(status || "").toLowerCase() || "pending";
  return `<td><span class="pill pill-${escapeHtml(normalized)}">${escapeHtml(normalized)}</span></td>`;
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

function setPending(id, key, value) {
  const state = getUiState(id);
  state.pending[key] = value;
  if (id === activeExtractorId) {
    renderActionButtons();
  }
  renderTabs();
}

function renderTabs() {
  tabList.innerHTML = extractorItems
    .map((item) => {
      const state = getUiState(item.id);
      const snapshot = state.latestState;
      const isActive = item.id === activeExtractorId;
      const isRunning = state.autoRun || snapshot?.runtime?.running;
      const statusClass = state.lastError ? "error" : isRunning ? "running" : "idle";
      const subtitle = state.lastError
        ? "Needs config"
        : snapshot
          ? `${snapshot.queue.pending} pending / ${snapshot.queue.failed} failed`
          : "Loading...";

      return `
        <button
          class="tab${isActive ? " active" : ""}"
          type="button"
          data-extractor-id="${escapeHtml(item.id)}"
        >
          <span class="tab-main">
            <span>${escapeHtml(item.label)}</span>
            <span class="status-dot status-${statusClass}"></span>
          </span>
          <span class="tab-subtle">${escapeHtml(subtitle)}</span>
        </button>
      `;
    })
    .join("");
}

function showControlSection(id) {
  for (const section of sections) {
    section.hidden = section.dataset.extractor !== id;
  }
}

function shouldAutoRunAfterSeed(id) {
  if (id === "foreplay") {
    return !forms.foreplay.manualBrandIds.value.trim();
  }
  if (id === "adplexity") {
    return !forms.adplexity.manualReportIds.value.trim();
  }
  return true;
}

function seedButtonLabel(id) {
  if (id === "meta") {
    return "Seed + Run";
  }
  return shouldAutoRunAfterSeed(id) ? "Seed + Run" : "Seed Queue";
}

function buildSeedPayload(id) {
  if (id === "foreplay") {
    return {
      months: forms.foreplay.months.value,
      manualBrandIds: forms.foreplay.manualBrandIds.value
    };
  }

  if (id === "adplexity") {
    return {
      manualReportIds: forms.adplexity.manualReportIds.value
    };
  }

  return {
    manualPageIds: forms.meta.manualPageIds.value,
    searchQuery: forms.meta.searchQuery.value,
    minDays: forms.meta.minDays.value,
    media: forms.meta.media.value,
    maxAdsPerPage: forms.meta.maxAdsPerPage.value
  };
}

function buildConfigNote(id, state) {
  if (!state) {
    return "State unavailable until the extractor secrets are configured.";
  }

  if (id === "foreplay") {
    return (
      `Checkpoint table: ${state.config.checkpointsTable}. ` +
      `Foreplay field: ${state.config.competitorsForeplayField}. ` +
      `Chunk budget: ${Math.round(state.config.chunkMaxMs / 1000)}s or ${state.config.chunkMaxCheckpoints} checkpoints. ` +
      `Retry limit: ${state.config.retryLimit}. Stale lock: ${state.config.staleMinutes} minutes.`
    );
  }

  if (id === "adplexity") {
    return (
      `Checkpoint table: ${state.config.checkpointsTable}. ` +
      `AdPlexity field: ${state.config.competitorsAdplexityField}. ` +
      `Chunk budget: ${Math.round(state.config.chunkMaxMs / 1000)}s or ${state.config.chunkMaxCheckpoints} checkpoints. ` +
      `Retry limit: ${state.config.retryLimit}. Stale lock: ${state.config.staleMinutes} minutes.`
    );
  }

  const bits = [
    `Checkpoint table: ${state.config.checkpointsTable}.`,
    `Defaults: min ${state.config.minDays} days, media ${state.config.media}, max ${state.config.maxAdsPerPage} ads/page, slices of ${state.config.sliceMaxAds}.`,
    `Chunk budget: ${Math.round(state.config.chunkMaxMs / 1000)}s or ${state.config.chunkMaxCheckpoints} checkpoints.`
  ];

  if (!state.config.metaFormTemplateConfigured) {
    bits.unshift("Missing `META_GRAPHQL_FORM_TEMPLATE` in Google AI Studio secrets.");
  } else if (!state.config.metaCookieConfigured) {
    bits.unshift("`META_COOKIE` is optional and only needed if Meta rejects body-only requests.");
  }

  return bits.join(" ");
}

function renderCheckpointTable(id, checkpoints) {
  const columns = checkpointColumns[id] || [];
  checkpointsHead.innerHTML = `<tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("")}</tr>`;

  if (!checkpoints.length) {
    checkpointsBody.innerHTML = `<tr><td colspan="${columns.length || 1}" class="empty">${escapeHtml(
      extractorMeta[id]?.emptyRowText || "No checkpoints yet."
    )}</td></tr>`;
    checkpointCount.textContent = "0 rows";
    return;
  }

  checkpointsBody.innerHTML = checkpoints
    .map((item) => `<tr>${columns.map((column) => column.render(item)).join("")}</tr>`)
    .join("");
  checkpointCount.textContent = `${checkpoints.length} rows shown`;
}

function renderSnapshot(state) {
  text("#total-count", state?.queue?.total ?? 0);
  text("#pending-count", state?.queue?.pending ?? 0);
  text("#running-count", state?.queue?.running ?? 0);
  text("#done-count", state?.queue?.done ?? 0);
  text("#failed-count", state?.queue?.failed ?? 0);
  text("#stale-count", state?.queue?.staleRunning ?? 0);
  text("#current-checkpoint", state?.runtime?.currentCheckpointKey || "None");
  text("#generated-at", state?.runtime?.generatedAt ? new Date(state.runtime.generatedAt).toLocaleString() : "-");

  queueJsonEl.textContent = JSON.stringify(
    {
      total: state?.queue?.total ?? 0,
      pending: state?.queue?.pending ?? 0,
      running: state?.queue?.running ?? 0,
      done: state?.queue?.done ?? 0,
      failed: state?.queue?.failed ?? 0,
      staleRunning: state?.queue?.staleRunning ?? 0,
      retryableFailed: state?.queue?.retryableFailed ?? 0
    },
    null,
    2
  );
}

function renderActionButtons() {
  const manifest = extractorItems.find((item) => item.id === activeExtractorId);
  const state = getUiState(activeExtractorId);
  const snapshot = state.latestState;
  const isRunning = state.autoRun || snapshot?.runtime?.running;
  const anyPending = Object.values(state.pending).some(Boolean);

  seedButton.textContent = seedButtonLabel(activeExtractorId);
  seedButton.disabled = Boolean(isRunning || state.pending.seed || state.pending.clear || state.pending.stop);
  runButton.disabled = Boolean(isRunning || state.pending.run || state.pending.clear || state.pending.stop);
  stopButton.disabled = Boolean(!isRunning || state.pending.stop);
  refreshButton.disabled = Boolean(state.pending.refresh);
  clearButton.disabled = Boolean(anyPending || isRunning);
  clearButton.hidden = !manifest?.supportsClearCheckpoints;
}

function renderCurrentView() {
  const manifest = extractorItems.find((item) => item.id === activeExtractorId);
  const state = getUiState(activeExtractorId);
  const snapshot = state.latestState;

  showControlSection(activeExtractorId);
  activeExtractorLabel.textContent = manifest?.label || activeExtractorId;
  extractorDescription.textContent = manifest?.description || "Extractor details unavailable.";

  if (!snapshot) {
    runtimeBadge.textContent = state.lastError ? "Unavailable" : "Loading...";
    configNote.textContent = buildConfigNote(activeExtractorId, null);
    renderSnapshot(null);
    logsEl.textContent = state.lastError || "Waiting for state...";
    queueJsonEl.textContent = state.lastError || "Waiting for queue state...";
    renderCheckpointTable(activeExtractorId, []);
    renderActionButtons();
    return;
  }

  runtimeBadge.textContent = snapshot.runtime.running
    ? `Running ${snapshot.runtime.currentAction || ""}`.trim()
    : state.autoRun
      ? "Auto-running"
      : "Idle";

  configNote.textContent = buildConfigNote(activeExtractorId, snapshot);
  logsEl.textContent =
    Array.isArray(snapshot.runtime.logs) && snapshot.runtime.logs.length
      ? snapshot.runtime.logs.join("\n")
      : "No logs yet.";

  renderSnapshot(snapshot);
  renderCheckpointTable(activeExtractorId, Array.isArray(snapshot.queue.checkpoints) ? snapshot.queue.checkpoints : []);
  renderActionButtons();
}

function setActiveExtractor(id) {
  activeExtractorId = id;
  renderTabs();
  renderCurrentView();
}

function storeState(id, state) {
  const ui = getUiState(id);
  ui.latestState = state;
  ui.lastError = "";
  renderTabs();
  if (id === activeExtractorId) {
    renderCurrentView();
  }
}

async function refreshState(id, { silent = false } = {}) {
  const ui = getUiState(id);
  if (!silent) {
    setPending(id, "refresh", true);
  }

  try {
    const payload = await request(`/api/extractors/${id}/state`);
    storeState(id, payload);
    return payload;
  } catch (error) {
    ui.lastError = error.message;
    if (id === activeExtractorId) {
      renderCurrentView();
    }
    throw error;
  } finally {
    if (!silent) {
      setPending(id, "refresh", false);
    }
  }
}

async function seedExtractor(id) {
  setPending(id, "seed", true);
  try {
    const payload = await request(`/api/extractors/${id}/seed`, {
      method: "POST",
      body: buildSeedPayload(id)
    });
    storeState(id, payload.state);
    return payload;
  } finally {
    setPending(id, "seed", false);
  }
}

async function runLoop(id) {
  const ui = getUiState(id);
  if (ui.autoRun) {
    return;
  }

  ui.autoRun = true;
  setPending(id, "run", true);
  renderCurrentView();

  try {
    while (ui.autoRun) {
      const payload = await request(`/api/extractors/${id}/run-chunk`, {
        method: "POST"
      });
      storeState(id, payload.state);

      if (!ui.autoRun) {
        break;
      }
      if (!payload.summary?.continueSuggested) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  } finally {
    ui.autoRun = false;
    setPending(id, "run", false);
    renderCurrentView();
  }
}

async function stopExtractor(id) {
  const ui = getUiState(id);
  ui.autoRun = false;
  setPending(id, "stop", true);

  try {
    const payload = await request(`/api/extractors/${id}/stop`, {
      method: "POST"
    });
    storeState(id, payload.state);
    return payload;
  } finally {
    setPending(id, "stop", false);
  }
}

async function waitForIdle(id, { timeoutMs = 120000, intervalMs = 1000 } = {}) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const state = await refreshState(id, { silent: true });
    if (!state.runtime.running) {
      return state;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error("Timed out waiting for the current run to stop.");
}

async function clearCheckpoints(id) {
  const ui = getUiState(id);
  setPending(id, "clear", true);

  try {
    if (ui.autoRun || ui.latestState?.runtime?.running) {
      await stopExtractor(id);
      await waitForIdle(id);
    }

    const payload = await request(`/api/extractors/${id}/clear-checkpoints`, {
      method: "POST"
    });
    storeState(id, payload.state);
    return payload;
  } finally {
    setPending(id, "clear", false);
  }
}

function handleActionError(error) {
  alert(error.message);
}

tabList.addEventListener("click", (event) => {
  const button = event.target.closest("[data-extractor-id]");
  if (!button) {
    return;
  }
  setActiveExtractor(button.dataset.extractorId);
});

seedButton.addEventListener("click", () => {
  const id = activeExtractorId;
  const autoRunAfterSeed = shouldAutoRunAfterSeed(id);

  seedExtractor(id)
    .then(async () => {
      if (!autoRunAfterSeed) {
        return;
      }
      await runLoop(id);
    })
    .catch(handleActionError);
});

runButton.addEventListener("click", () => {
  runLoop(activeExtractorId).catch(handleActionError);
});

stopButton.addEventListener("click", () => {
  stopExtractor(activeExtractorId).catch(handleActionError);
});

refreshButton.addEventListener("click", () => {
  refreshState(activeExtractorId).catch(handleActionError);
});

clearButton.addEventListener("click", () => {
  clearCheckpoints(activeExtractorId).catch(handleActionError);
});

forms.foreplay.manualBrandIds.addEventListener("input", () => {
  if (activeExtractorId === "foreplay") {
    renderActionButtons();
  }
});

forms.adplexity.manualReportIds.addEventListener("input", () => {
  if (activeExtractorId === "adplexity") {
    renderActionButtons();
  }
});

async function initialize() {
  const payload = await request("/api/extractors");
  extractorItems = Array.isArray(payload.items) ? payload.items : [];
  for (const item of extractorItems) {
    getUiState(item.id);
  }

  if (extractorItems.length) {
    activeExtractorId = extractorItems[0].id;
  }

  renderTabs();
  renderCurrentView();

  await Promise.all(
    extractorItems.map((item) =>
      refreshState(item.id, { silent: true }).catch((error) => {
        const ui = getUiState(item.id);
        ui.lastError = error.message;
      })
    )
  );

  renderTabs();
  renderCurrentView();
}

setInterval(() => {
  if (document.hidden || !activeExtractorId) {
    return;
  }

  const ui = getUiState(activeExtractorId);
  if (!ui.autoRun) {
    refreshState(activeExtractorId, { silent: true }).catch(() => undefined);
  }
}, 5000);

initialize().catch((error) => {
  logsEl.textContent = error.message;
  queueJsonEl.textContent = error.message;
});
