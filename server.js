import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { getExtractor, listExtractors } from "./src/extractors.js";
import { readJsonBody, sendJson, sendStaticFile } from "./src/server-utils.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, "public");
const port = Number.parseInt(String(process.env.PORT ?? "8080"), 10) || 8080;

function matchExtractorRoute(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  if (parts.length < 3 || parts[0] !== "api" || parts[1] !== "extractors") {
    return null;
  }

  return {
    extractorId: parts[2],
    action: parts[3] || ""
  };
}

export const server = http.createServer(async (req, res) => {
  let extractor = null;

  try {
    if (!req.url) {
      sendJson(res, 400, { error: "Missing request URL." });
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host || "127.0.0.1"}`);
    const extractorRoute = matchExtractorRoute(url.pathname);
    if (extractorRoute) {
      extractor = getExtractor(extractorRoute.extractorId);
      if (!extractor) {
        sendJson(res, 404, { error: `Unknown extractor: ${extractorRoute.extractorId}` });
        return;
      }
    }

    if (req.method === "GET" && url.pathname === "/api/health") {
      sendJson(res, 200, {
        ok: true,
        app: "google-ai-studio-combined-extractors-v1",
        mode: "combined-foreplay-adplexity-meta",
        extractors: listExtractors()
      });
      return;
    }

    if (req.method === "GET" && url.pathname === "/api/extractors") {
      sendJson(res, 200, {
        items: listExtractors()
      });
      return;
    }

    if (extractorRoute && req.method === "GET" && extractorRoute.action === "state") {
      sendJson(res, 200, await extractor.getState());
      return;
    }

    if (extractorRoute && req.method === "POST" && extractorRoute.action === "seed") {
      const body = await readJsonBody(req);
      const summary = await extractor.seed(body);
      sendJson(res, 200, {
        ok: true,
        summary,
        state: await extractor.getState()
      });
      return;
    }

    if (extractorRoute && req.method === "POST" && extractorRoute.action === "run-chunk") {
      if (extractor.runtime.state.running) {
        sendJson(res, 409, {
          error: "A chunk is already running.",
          state: await extractor.getState()
        });
        return;
      }

      const body = await readJsonBody(req);
      const summary = await extractor.runChunk(body);
      sendJson(res, 200, {
        ok: true,
        summary,
        state: await extractor.getState()
      });
      return;
    }

    if (extractorRoute && req.method === "POST" && extractorRoute.action === "stop") {
      extractor.requestStop();
      sendJson(res, 200, {
        ok: true,
        state: await extractor.getState()
      });
      return;
    }

    if (extractorRoute && req.method === "POST" && extractorRoute.action === "clear-checkpoints") {
      if (extractor.runtime.state.running) {
        sendJson(res, 409, {
          error: "Stop the current run before clearing checkpoints.",
          state: await extractor.getState()
        });
        return;
      }

      const summary = await extractor.clearCheckpoints();
      sendJson(res, 200, {
        ok: true,
        summary,
        state: await extractor.getState()
      });
      return;
    }

    if (req.method === "GET" && url.pathname === "/") {
      await sendStaticFile(res, path.join(publicDir, "index.html"), "text/html; charset=utf-8");
      return;
    }

    if (req.method === "GET" && (url.pathname === "/app.js" || url.pathname === "/styles.css")) {
      const filePath = path.join(publicDir, url.pathname.slice(1));
      const contentType =
        url.pathname === "/app.js" ? "application/javascript; charset=utf-8" : "text/css; charset=utf-8";
      await sendStaticFile(res, filePath, contentType);
      return;
    }

    sendJson(res, 404, { error: "Not found." });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (extractor?.runtime?.log) {
      extractor.runtime.log("Server error:", message);
    } else {
      console.error(message);
    }
    sendJson(res, 500, { error: message });
  }
});

server.listen(port, async () => {
  try {
    await fs.access(publicDir);
  } catch {
    console.log("Warning: public directory is missing.");
  }

  console.log(`Combined extractors dashboard listening on http://127.0.0.1:${port}`);
});
