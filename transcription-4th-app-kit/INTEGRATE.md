# Transcription 4th App Kit

This folder is a handoff kit for manually adding the working transcription app as a 4th extractor inside:

`C:\Animation\Projects\Competitors Extractor - Copy\combined-extractors-dashboard`

It does **not** modify the combined dashboard by itself.

## What To Copy

Copy this folder:

`src\extractors\transcription`

into:

`combined-extractors-dashboard\src\extractors\transcription`

The copied `config.js` in this kit is already adjusted to the combined app folder depth, so it will load the combined app `.env`.

## What Is In `reference`

These files are reference-only and are here so you can manually merge the transcription-specific browser flow into the combined dashboard:

- `reference\server.js`
- `reference\public\app.js`
- `reference\public\index.html`

Use them to copy over:

- browser Gemini config injection into HTML
- browser-task routes
- retry-failed route
- transcription-specific frontend run loop
- transcription-specific UI copy

Do **not** replace the combined dashboard files wholesale unless you want the standalone transcription UI.

## Combined App Files You Will Need To Edit Manually

- `combined-extractors-dashboard\src\extractors.js`
- `combined-extractors-dashboard\server.js`
- `combined-extractors-dashboard\public\app.js`
- `combined-extractors-dashboard\public\index.html`

## Important Dependency Note

The copied module imports:

- `@google/genai`

So if you copy `src\extractors\transcription` into the combined app, also add this dependency to the combined app `package.json` and reinstall:

```json
{
  "dependencies": {
    "@google/genai": "^1.49.0"
  }
}
```

If the combined app already has a `dependencies` block, just merge the package into it.

## Env Vars Needed In The Combined App

Keep using the same env names from the standalone app:

- `TRANSCRIPTION_AIRTABLE_*`
- `GEMINI_API_KEY`
- `GEMINI_MODEL`
- `GEMINI_IMAGE_MODEL`

You can use the included `.env.example` as the transcription-side reference.

## Folder Layout In This Kit

- `src\extractors\transcription`
  Ready to copy into the combined app.
- `reference`
  Working standalone server/frontend files to use as merge references.
- `.env.example`
  Reference env names for the transcription module.

## Recommended Manual Merge Order

1. Copy `src\extractors\transcription` into the combined app.
2. Add `@google/genai` to the combined app `package.json`.
3. Add a `transcription` extractor wrapper in `combined-extractors-dashboard\src\extractors.js`.
4. Add the transcription browser-task routes from `reference\server.js`.
5. Add the transcription frontend loop and UI wiring from `reference\public\app.js` and `reference\public\index.html`.
6. Add the transcription env vars to the combined app.
7. Test the 4th tab inside the combined dashboard.
