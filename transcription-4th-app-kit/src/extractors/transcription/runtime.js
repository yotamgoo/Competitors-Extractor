import { nowIso } from "./utils.js";

export function createRuntime() {
  const state = {
    running: false,
    stopRequested: false,
    currentAction: "",
    currentCheckpointKey: "",
    logs: []
  };

  function log(...parts) {
    const text = parts.map((part) => String(part)).join(" ").trim();
    if (!text) {
      return;
    }
    const line = `[${new Date().toTimeString().slice(0, 8)}] ${text}`;
    state.logs = [...state.logs.slice(-199), line];
  }

  return {
    state,
    log,
    begin(action) {
      state.running = true;
      state.stopRequested = false;
      state.currentAction = action;
      state.currentCheckpointKey = "";
      log(`Started ${action}.`);
    },
    finish(action) {
      state.running = false;
      state.currentAction = "";
      state.currentCheckpointKey = "";
      state.stopRequested = false;
      log(`Finished ${action}.`);
    },
    requestStop() {
      if (!state.stopRequested) {
        state.stopRequested = true;
        log("Stop requested. Current record will finish before the queue pauses.");
      }
    },
    setCheckpoint(key) {
      state.currentCheckpointKey = key || "";
    },
    snapshot() {
      return {
        running: state.running,
        stopRequested: state.stopRequested,
        currentAction: state.currentAction,
        currentCheckpointKey: state.currentCheckpointKey,
        generatedAt: nowIso(),
        logs: [...state.logs]
      };
    }
  };
}
