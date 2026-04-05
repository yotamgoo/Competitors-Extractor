# Setup Guide

This app is designed to stay simple:

- one Node app
- one dashboard UI
- no extra infrastructure
- secrets re-entered manually when needed

## 1. Put It In A Private Git Repo

Inside this folder:

```bash
git init -b main
git add .
git commit -m "Initial combined extractors dashboard"
```

Then create a private remote repository and connect it:

```bash
git remote add origin <your-private-repo-url>
git push -u origin main
```

On your work laptop:

```bash
git clone <your-private-repo-url>
cd combined-extractors-dashboard
```

## 2. Install And Run Locally If You Want

This app has no external runtime build step.

```bash
npm start
```

By default it runs on port `8080`.

## 3. Secrets Strategy

Do not commit real secrets.

Use [`.env.example`](/C:/Animation/Projects/Competitors%20Extractor%20-%20Copy/combined-extractors-dashboard/.env.example) as the reference shape, then create your own local `.env` or enter the same values directly in Google AI Studio secrets.

Important:

- Keep Meta cookies, `fb_dtsg`, `lsd`, Airtable PATs, and passwords out of Git.
- Store those values in a password manager or a private secure note instead.
- The values you pasted in chat should be treated as sensitive session secrets and should not be written into the repo.

## 4. What To Enter

### Shared defaults

These can be set once if all extractors share the same Airtable base and general chunk settings:

- `AIRTABLE_PAT`
- `AIRTABLE_BASE_ID`
- `AIRTABLE_COMPETITORS_TABLE`
- `AIRTABLE_ADS_TABLE`
- `AIRTABLE_COMPETITORS_ACTIVE_FIELD`
- `AIRTABLE_COMPETITORS_NAME_FIELD`
- `CHUNK_MAX_MS`
- `CHUNK_MAX_CHECKPOINTS`
- `CHECKPOINT_RETRY_LIMIT`
- `CHECKPOINT_STALE_MINUTES`

### Foreplay

- `FOREPLAY_EMAIL`
- `FOREPLAY_PASSWORD`
- Optional extractor-specific Airtable overrides:
  - `FOREPLAY_AIRTABLE_PAT`
  - `FOREPLAY_AIRTABLE_BASE_ID`
  - `FOREPLAY_AIRTABLE_COMPETITORS_TABLE`
  - `FOREPLAY_AIRTABLE_ADS_TABLE`
  - `FOREPLAY_AIRTABLE_CHECKPOINTS_TABLE`
  - `FOREPLAY_AIRTABLE_COMPETITORS_ACTIVE_FIELD`
  - `FOREPLAY_AIRTABLE_COMPETITORS_NAME_FIELD`
  - `FOREPLAY_AIRTABLE_COMPETITORS_FOREPLAY_FIELD`

### AdPlexity

- `ADPLEXITY_EMAIL`
- `ADPLEXITY_PASSWORD`
- Optional extractor-specific Airtable overrides:
  - `ADPLEXITY_AIRTABLE_PAT`
  - `ADPLEXITY_AIRTABLE_BASE_ID`
  - `ADPLEXITY_AIRTABLE_COMPETITORS_TABLE`
  - `ADPLEXITY_AIRTABLE_ADS_TABLE`
  - `ADPLEXITY_AIRTABLE_CHECKPOINTS_TABLE`
  - `ADPLEXITY_AIRTABLE_COMPETITORS_ACTIVE_FIELD`
  - `ADPLEXITY_AIRTABLE_COMPETITORS_NAME_FIELD`
  - `ADPLEXITY_AIRTABLE_COMPETITORS_ADPLEXITY_FIELD`

### Meta

Required or commonly needed:

- `META_AIRTABLE_PAT` or shared `AIRTABLE_PAT`
- `META_AIRTABLE_BASE_ID` or shared `AIRTABLE_BASE_ID`
- `META_AIRTABLE_CHECKPOINTS_TABLE`
- `META_AIRTABLE_COMPETITORS_META_PAGE_FIELD`
- `META_GRAPHQL_FORM_TEMPLATE`

Usually needed for a real signed-in session:

- `META_COOKIE`
- `META_FB_DTSG`
- `META_X_FB_LSD`
- `META_X_ASBD_ID`

Optional tuning:

- `META_MIN_DAYS`
- `META_MEDIA`
- `META_MAX_ADS_PER_PAGE`
- `META_SLICE_MAX_ADS`
- `META_GRAPHQL_API_URL`
- `META_GRAPHQL_DOC_ID`
- `META_GRAPHQL_FRIENDLY_NAME`
- `META_ACCEPT_LANGUAGE`
- `META_USER_AGENT`
- `META_REQUEST_TIMEOUT_MS`

## 5. Meta Template Mapping

When you collect a fresh Meta request from DevTools or a HAR:

- Put the full form-encoded request body into `META_GRAPHQL_FORM_TEMPLATE`.
- Put the browser cookie header into `META_COOKIE` if needed.
- Put the request `fb_dtsg` value into `META_FB_DTSG`.
- Put the request `lsd` value into `META_X_FB_LSD`.
- Put the request `x-asbd-id` value into `META_X_ASBD_ID`.

Use a fresh session if Meta starts rejecting requests.

## 6. Google AI Studio Upload

Upload just this folder as the single app:

- [combined-extractors-dashboard](/C:/Animation/Projects/Competitors%20Extractor%20-%20Copy/combined-extractors-dashboard)

Then re-enter the secrets manually in AI Studio.

## 7. Sanity Check After Setup

After secrets are in place:

1. Open the app.
2. Click each tab: Foreplay, AdPlexity, Meta.
3. Confirm state loads instead of showing missing-secret errors.
4. Try `Refresh` first.
5. Then test a small seed/run flow per extractor.

## 8. Safe Ongoing Workflow

Suggested workflow from now on:

```bash
git status
git add .
git commit -m "Describe the change"
git push
```

If you refresh Meta session values, update them only in your local secrets or AI Studio secrets, not in Git.
