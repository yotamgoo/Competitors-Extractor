import { sleep } from "./utils.js";

const API_BASE = "https://api.foreplay.co";
const PAGE_SIZE = 100;
const MAX_RETRIES = 3;
const RATE_LIMIT_BUFFER = 5;
const FIREBASE_API_KEY = "AIzaSyCIn3hB6C5qsx5L_a_V17n08eJ24MeqYDg";
const FIREBASE_VERIFY_URL = `https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key=${FIREBASE_API_KEY}`;
const FIREBASE_REFRESH_URL = `https://securetoken.googleapis.com/v1/token?key=${FIREBASE_API_KEY}`;
const FIRESTORE_RUN_QUERY_URL =
  "https://firestore.googleapis.com/v1/projects/adison-foreplay/databases/(default)/documents:runQuery";
const FIRESTORE_BRAND_DOCS_BASE_URL =
  "https://firestore.googleapis.com/v1/projects/adison-foreplay/databases/(default)/documents/brands";

const DEFAULT_HEADERS = {
  accept: "application/json, text/plain, */*",
  "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
  "cache-control": "no-cache",
  dnt: "1",
  origin: "https://app.foreplay.co",
  pragma: "no-cache",
  referer: "https://app.foreplay.co/",
  "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
  "sec-ch-ua-mobile": "?0",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
  "user-agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
};

const DAY_MS = 24 * 60 * 60 * 1000;

export class ForeplayClient {
  constructor(email, password, log = console.log) {
    this.email = email;
    this.password = password;
    this.log = log;
    this.refreshToken = null;
    this.tokenExpiresAt = 0;
    this.authHeaders = { ...DEFAULT_HEADERS };
  }

  async initialize() {
    this.log("Authenticating with Foreplay...");
    const auth = await this.firebaseLogin(this.email, this.password);
    this.authHeaders.authorization = `Bearer ${auth.idToken}`;
    this.refreshToken = auth.refreshToken || null;
    this.tokenExpiresAt = Date.now() + Number(auth.expiresIn || "3600") * 1000 - 60000;
    this.log("Foreplay authentication succeeded.");
  }

  async getCreativeTestDates(brandId) {
    const aggregations = [];
    let cursor = null;

    while (true) {
      const response = await this.request(
        "GET",
        `/brands/creative-tests/${brandId}`,
        cursor ? { next: cursor } : undefined
      );
      const batch = Array.isArray(response.aggregations) ? response.aggregations : [];
      aggregations.push(...batch);

      if (!response.nextId || !batch.length) {
        break;
      }
      cursor = response.nextId;
    }

    return aggregations;
  }

  async *iterBrands() {
    let cursor = null;

    while (true) {
      const response = await this.request(
        "GET",
        "/brands/discovery",
        cursor ? { sort: "subscriberCount", next: cursor } : { sort: "subscriberCount" }
      );
      const results = Array.isArray(response.results) ? response.results : [];
      if (!results.length) {
        break;
      }

      for (const brand of results) {
        yield brand;
      }

      if (!response.nextPage) {
        break;
      }

      cursor = typeof response.nextPage === "string" ? response.nextPage : JSON.stringify(response.nextPage);
    }
  }

  async *iterAds(options) {
    const params = {
      "orBrands[]": options.brandId,
      sort: "longest",
      spyder: "true",
      size: String(PAGE_SIZE)
    };

    if (typeof options.startedAfter === "number") {
      params.startedRunningStart = String(options.startedAfter);
    }
    if (typeof options.startedBefore === "number") {
      params.startedRunningEnd = String(options.startedBefore);
    }

    let cursor = null;
    let page = 0;
    while (true) {
      page += 1;
      const response = await this.request(
        "GET",
        "/ads/discovery",
        cursor ? { ...params, next: cursor } : params
      );
      const results = Array.isArray(response.results) ? response.results : [];
      if (!results.length) {
        break;
      }

      this.log(`  Foreplay ads page ${page}: ${results.length} records`);
      for (const ad of results) {
        yield ad;
      }

      if (!response.nextPage) {
        break;
      }
      cursor = String(response.nextPage);
    }
  }

  async getDcoThumbnail(options) {
    const extractImage = (results, targetFbAdId) => {
      for (const ad of results) {
        if (targetFbAdId && ad.ad_id !== targetFbAdId) {
          continue;
        }
        const firstCard = Array.isArray(ad.cards) ? ad.cards[0] : null;
        const url = firstCard?.thumbnail || firstCard?.image || ad.image;
        if (url) {
          return url;
        }
      }
      return null;
    };

    try {
      if (options.collationId) {
        const response = await this.request("GET", "/ads/discovery", {
          "orBrands[]": options.brandId,
          collationId: options.collationId
        });
        const match = extractImage(response.results || [], null);
        if (match) {
          return match;
        }
      }

      if (options.fbAdId && options.startedRunning) {
        const response = await this.request("GET", "/ads/discovery", {
          "orBrands[]": options.brandId,
          startedRunningStart: String(options.startedRunning),
          startedRunningEnd: String(options.startedRunning + DAY_MS - 1),
          spyder: "true",
          size: "100"
        });
        const match = extractImage(response.results || [], options.fbAdId);
        if (match) {
          return match;
        }
      }
    } catch {
      return null;
    }

    return null;
  }

  async resolveBrandIdFromPageId(pageId) {
    const normalizedPageId = String(pageId ?? "").trim();
    if (!normalizedPageId || !/^\d+$/.test(normalizedPageId)) {
      return null;
    }

    await this.ensureToken();

    const payload = {
      structuredQuery: {
        from: [{ collectionId: "fb_ads_page_track" }],
        where: {
          fieldFilter: {
            field: { fieldPath: "pageId" },
            op: "EQUAL",
            value: { stringValue: normalizedPageId }
          }
        },
        limit: 1
      }
    };

    let lastError = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      try {
        const response = await fetch(FIRESTORE_RUN_QUERY_URL, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            origin: "https://app.foreplay.co",
            referer: "https://app.foreplay.co/",
            authorization: this.authHeaders.authorization
          },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(20000)
        });

        if (response.status >= 500) {
          lastError = new Error(`Firestore server error ${response.status}`);
          await sleep(2 ** attempt * 1000);
          continue;
        }

        if (!response.ok) {
          throw new Error(`Firestore runQuery failed: ${response.status}`);
        }

        const rows = await response.json();
        for (const row of rows) {
          const brandId = row?.document?.fields?.brandId?.stringValue?.trim();
          if (brandId) {
            return brandId;
          }
        }
        return null;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        await sleep(2 ** attempt * 1000);
      }
    }

    this.log(`Foreplay page-id lookup failed: ${lastError?.message || "unknown error"}`);
    return null;
  }

  async resolveBrandNameFromBrandId(brandId) {
    const normalizedBrandId = String(brandId ?? "").trim();
    if (!normalizedBrandId) {
      return null;
    }

    await this.ensureToken();
    const endpoint = `${FIRESTORE_BRAND_DOCS_BASE_URL}/${encodeURIComponent(normalizedBrandId)}`;

    let lastError = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      try {
        const response = await fetch(endpoint, {
          method: "GET",
          headers: {
            origin: "https://app.foreplay.co",
            referer: "https://app.foreplay.co/",
            authorization: this.authHeaders.authorization
          },
          signal: AbortSignal.timeout(20000)
        });

        if (response.status === 404) {
          return null;
        }
        if (response.status >= 500) {
          lastError = new Error(`Firestore brand lookup server error ${response.status}`);
          await sleep(2 ** attempt * 1000);
          continue;
        }
        if (!response.ok) {
          throw new Error(`Firestore brand lookup failed: ${response.status}`);
        }

        const doc = await response.json();
        return doc?.fields?.name?.stringValue?.trim() || doc?.fields?.sortName?.stringValue?.trim() || null;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        await sleep(2 ** attempt * 1000);
      }
    }

    this.log(`Foreplay brand-name lookup failed: ${lastError?.message || "unknown error"}`);
    return null;
  }

  async ensureToken() {
    if (!this.refreshToken || Date.now() < this.tokenExpiresAt) {
      return;
    }

    const body = new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: this.refreshToken
    });
    const response = await fetch(FIREBASE_REFRESH_URL, {
      method: "POST",
      body,
      headers: {
        origin: "https://app.foreplay.co",
        referer: "https://app.foreplay.co/",
        "x-client-version": "Chrome/JsCore/8.10.1/FirebaseCore-web"
      }
    });

    if (!response.ok) {
      throw new Error(`Foreplay token refresh failed: ${response.status}`);
    }

    const data = await response.json();
    const nextToken = data.id_token || data.access_token;
    if (!nextToken) {
      throw new Error("Foreplay token refresh did not return a token.");
    }

    this.authHeaders.authorization = `Bearer ${nextToken}`;
    this.refreshToken = data.refresh_token || this.refreshToken;
    this.tokenExpiresAt = Date.now() + Number(data.expires_in || "3600") * 1000 - 60000;
  }

  async request(method, path, params) {
    await this.ensureToken();

    const url = new URL(`${API_BASE}${path}`);
    for (const [key, value] of Object.entries(params || {})) {
      url.searchParams.append(key, value);
    }

    let lastError = null;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
      try {
        const response = await fetch(url, {
          method,
          headers: this.authHeaders,
          signal: AbortSignal.timeout(30000)
        });

        const remaining = response.headers.get("x-ratelimit-remaining");
        const resetAt = response.headers.get("x-ratelimit-reset");
        if (remaining !== null && Number(remaining) <= RATE_LIMIT_BUFFER) {
          const waitMs = Math.max(0, Number(resetAt || "0") * 1000 - Date.now()) + 2000;
          this.log(`Foreplay rate-limit low (${remaining} left), sleeping ${Math.round(waitMs / 1000)}s`);
          await sleep(waitMs);
        }

        if (response.status === 429) {
          const waitMs = Math.max(0, Number(resetAt || "0") * 1000 - Date.now()) + 2000;
          lastError = new Error("Foreplay rate limited");
          await sleep(waitMs);
          continue;
        }

        if (response.status >= 500) {
          lastError = new Error(`Foreplay server error ${response.status}`);
          await sleep(2 ** attempt * 1000);
          continue;
        }

        if (!response.ok) {
          throw new Error(`Foreplay request failed: ${response.status}`);
        }

        return await response.json();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt >= MAX_RETRIES) {
          break;
        }
        await sleep(2 ** attempt * 1000);
      }
    }

    throw new Error(`Foreplay request failed after ${MAX_RETRIES} attempts: ${lastError?.message || "unknown error"}`);
  }

  async firebaseLogin(email, password) {
    const response = await fetch(FIREBASE_VERIFY_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        origin: "https://app.foreplay.co",
        referer: "https://app.foreplay.co/"
      },
      body: JSON.stringify({
        email,
        password,
        returnSecureToken: true
      }),
      signal: AbortSignal.timeout(15000)
    });

    if (!response.ok) {
      throw new Error(`Foreplay Firebase login failed: ${response.status}`);
    }

    return await response.json();
  }
}
