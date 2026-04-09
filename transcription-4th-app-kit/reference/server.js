import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";

import { config } from "./src/config.js";
import { createTranscriptionApp } from "./src/transcription-app.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, "public");
const port = config.port;
const app = express();
const transcriptionApp = createTranscriptionApp();

function serializeScriptValue(value) {
  return JSON.stringify(value).replaceAll("</", "<\\/");
}

app.use(express.json({ limit: "1mb" }));
app.use(express.static(publicDir, { index: false }));

app.get("/api/health", (_req, res) => {
  res.status(200).json({
    ok: true,
    app: "google-ai-studio-video-transcription-v1",
    mode: "airtable-seed-queue-gemini-video-transcription"
  });
});

app.get("/api/state", async (_req, res, next) => {
  try {
    res.status(200).json(await transcriptionApp.getState());
  } catch (error) {
    next(error);
  }
});

app.post("/api/seed", async (_req, res, next) => {
  try {
    if (transcriptionApp.runtime.state.running) {
      res.status(409).json({
        error: "Stop the current run before seeding the queue.",
        state: await transcriptionApp.getState()
      });
      return;
    }

    const summary = await transcriptionApp.seed();
    res.status(200).json({
      ok: true,
      summary,
      state: await transcriptionApp.getState()
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/run-chunk", async (_req, res, next) => {
  try {
    if (transcriptionApp.runtime.state.running) {
      res.status(409).json({
        error: "A chunk is already running.",
        state: await transcriptionApp.getState()
      });
      return;
    }

    const summary = await transcriptionApp.runChunk();
    res.status(200).json({
      ok: true,
      summary,
      state: await transcriptionApp.getState()
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/claim-browser-task", async (_req, res, next) => {
  try {
    if (transcriptionApp.runtime.state.stopRequested && !transcriptionApp.runtime.state.running) {
      transcriptionApp.runtime.state.stopRequested = false;
    }

    const payload = await transcriptionApp.claimBrowserTask();
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

app.post("/api/complete-browser-task", async (req, res, next) => {
  try {
    const payload = await transcriptionApp.completeBrowserTask({
      recordId: req.body?.recordId,
      sourceAdRecordId: req.body?.sourceAdRecordId,
      transcript: req.body?.transcript,
      bytes: req.body?.bytes
    });
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

app.post("/api/fail-browser-task", async (req, res, next) => {
  try {
    const payload = await transcriptionApp.failBrowserTask({
      recordId: req.body?.recordId,
      key: req.body?.key,
      error: req.body?.error,
      fatal: Boolean(req.body?.fatal)
    });
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

app.post("/api/finish-browser-run", async (req, res, next) => {
  try {
    const payload = await transcriptionApp.finishBrowserRun({
      processed: req.body?.processed,
      done: req.body?.done,
      failed: req.body?.failed,
      reason: req.body?.reason
    });
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

app.post("/api/stop", async (_req, res, next) => {
  try {
    transcriptionApp.requestStop();
    res.status(200).json({
      ok: true,
      state: await transcriptionApp.getState()
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/retry-failed", async (_req, res, next) => {
  try {
    if (transcriptionApp.runtime.state.running) {
      res.status(409).json({
        error: "Stop the current run before retrying failed rows.",
        state: await transcriptionApp.getState()
      });
      return;
    }

    const summary = await transcriptionApp.retryFailed();
    res.status(200).json({
      ok: true,
      summary,
      state: await transcriptionApp.getState()
    });
  } catch (error) {
    next(error);
  }
});

app.get("/", async (_req, res, next) => {
  try {
    const body = await fs.readFile(path.join(publicDir, "index.html"), "utf8");
    const injected = body.replace(
      "</body>",
      `<script>window.__APP_CONFIG__=${serializeScriptValue({
        browserGemini: {
          apiKey: config.gemini.apiKey,
          apiUrl: config.gemini.apiUrl,
          model: config.gemini.model,
          imageModel: config.gemini.imageModel,
          prompt: config.gemini.prompt,
          imagePrompt: config.gemini.imagePrompt,
          language: config.gemini.language,
          maxOutputTokens: config.gemini.maxOutputTokens,
          temperature: config.gemini.temperature,
          timeoutMs: config.gemini.timeoutMs,
          maxInlineBytes: config.runtime.maxInlineBytes
        }
      })};</script></body>`
    );
    res.type("html").send(injected);
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  const message = error instanceof Error ? error.message : String(error);
  transcriptionApp.runtime.log("Server error:", message);
  res.status(500).json({ error: message });
});

app.listen(port, async () => {
  try {
    await fs.access(publicDir);
  } catch {
    console.log("Warning: public directory is missing.");
  }

  console.log(`Video transcription dashboard listening on http://127.0.0.1:${port}`);
});
