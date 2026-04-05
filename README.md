# Combined Extractors Dashboard

This folder is a thin Google-AI-Studio-friendly wrapper around the three working extractor apps in the workspace:

- Foreplay
- AdPlexity
- Meta

The original apps stay untouched. This combined app copies their extractor code into `src/extractors/*` and adds one shared Node server plus one shared dashboard UI.

## Run

```bash
npm start
```

The app uses the same simple runtime shape as the original projects: one `server.js`, one `public` folder, and no extra infrastructure.

## Environment Variables

- Shared Airtable defaults can be entered once with `AIRTABLE_*` and `CHUNK_*`.
- Each extractor can override those shared defaults with its own namespaced variables such as `FOREPLAY_AIRTABLE_*`, `ADPLEXITY_AIRTABLE_*`, and `META_AIRTABLE_*`.
- Extractor-specific credentials stay separate:
  - `FOREPLAY_EMAIL`, `FOREPLAY_PASSWORD`
  - `ADPLEXITY_EMAIL`, `ADPLEXITY_PASSWORD`
  - `META_GRAPHQL_*`, `META_COOKIE`, `META_FB_DTSG`, `META_X_FB_LSD`

Start from `.env.example` when re-entering secrets in Google AI Studio.

## Notes

- Each extractor keeps its own runtime state, resumable checkpoints, and Airtable writes.
- Each extractor can be seeded, run, stopped, refreshed, and cleared independently from one UI.
- The copied `scripts/extract-meta-session.js` helper is included only as a reference utility; it is not required for runtime.
