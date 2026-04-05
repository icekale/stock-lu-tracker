const { createHash } = require("node:crypto");

const SECURITY_NAME_CACHE_TTL_MS = Math.max(
  0,
  Number(process.env.SECURITY_NAME_CACHE_TTL_MS) || 12 * 60 * 60 * 1000
);
const SECURITY_NAME_FAILURE_CACHE_TTL_MS = Math.max(
  0,
  Number(process.env.SECURITY_NAME_FAILURE_CACHE_TTL_MS) || 2 * 60 * 1000
);
const securityNameCache = new Map();
const securityNameInFlight = new Map();

function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function hashText(value) {
  return createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

function buildSecurityNameCacheKey(apiSymbol, options = {}) {
  const symbol = String(apiSymbol || "").trim().toUpperCase();
  const xueqiuCookie = String(options?.xueqiuCookie || "").trim();

  return JSON.stringify({
    apiSymbol: symbol,
    xueqiuCookieHash: xueqiuCookie ? hashText(xueqiuCookie) : ""
  });
}

function toTencentCode(apiSymbol) {
  const symbol = String(apiSymbol || "").toUpperCase();

  if (symbol.endsWith(".SS")) {
    return `sh${symbol.replace(".SS", "")}`;
  }

  if (symbol.endsWith(".SZ")) {
    return `sz${symbol.replace(".SZ", "")}`;
  }

  if (symbol.endsWith(".HK")) {
    const raw = symbol.replace(".HK", "");
    return `hk${raw.padStart(5, "0")}`;
  }

  const usCode = symbol.replace(/-/g, ".");
  return `us${usCode}`;
}

function toXueqiuSearchCode(apiSymbol) {
  const symbol = String(apiSymbol || "").toUpperCase().trim();

  if (symbol.endsWith(".SS")) {
    return `SH${symbol.replace(".SS", "")}`;
  }

  if (symbol.endsWith(".SZ")) {
    return `SZ${symbol.replace(".SZ", "")}`;
  }

  if (symbol.endsWith(".HK")) {
    return symbol.replace(".HK", "").padStart(5, "0");
  }

  return symbol.replace(/-/g, ".");
}

function buildXueqiuHeaders(cookie) {
  return {
    Cookie: String(cookie || "").trim(),
    Referer: "https://xueqiu.com/",
    Accept: "application/json,text/plain,*/*",
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
  };
}

function matchesXueqiuSearchResult(apiSymbol, stock) {
  const searchCode = toXueqiuSearchCode(apiSymbol);
  const code = String(stock?.code || "").toUpperCase().trim();
  if (!searchCode || !code) {
    return false;
  }

  if (code === searchCode) {
    return true;
  }

  if (String(apiSymbol || "").toUpperCase().endsWith(".HK")) {
    return code === searchCode.padStart(5, "0");
  }

  return false;
}

async function fetchOneXueqiuSecurityName(apiSymbol, cookie) {
  const searchCode = toXueqiuSearchCode(apiSymbol);
  if (!searchCode || !cookie) {
    return "";
  }

  const url = `https://xueqiu.com/stock/search.json?size=10&code=${encodeURIComponent(searchCode)}`;
  const response = await fetch(url, {
    headers: buildXueqiuHeaders(cookie)
  });

  if (!response.ok) {
    throw new Error(`雪球名称请求失败 (${response.status})`);
  }

  const payload = await response.json();
  const stocks = Array.isArray(payload?.stocks) ? payload.stocks : [];
  const exactMatch = stocks.find((item) => matchesXueqiuSearchResult(apiSymbol, item)) || stocks[0];
  return String(exactMatch?.name || "").trim();
}

function guessCurrency(apiSymbol, fields) {
  const explicitCurrency = fields.find((item) => /^[A-Z]{3}$/.test(item));
  if (explicitCurrency) {
    return explicitCurrency;
  }

  const symbol = String(apiSymbol || "").toUpperCase();
  if (symbol.endsWith(".HK")) {
    return "HKD";
  }
  if (symbol.endsWith(".SS") || symbol.endsWith(".SZ")) {
    return "CNY";
  }
  return "USD";
}

function parseTencentPayload(rawText) {
  const matched = rawText.match(/=\"([^\"]*)\"/);
  if (!matched) {
    throw new Error("行情返回格式异常");
  }

  return matched[1].split("~");
}

async function readResponseText(response) {
  const bytes = Buffer.from(await response.arrayBuffer());
  if (bytes.length === 0) {
    return "";
  }

  try {
    return new TextDecoder("gb18030").decode(bytes);
  } catch {
    return bytes.toString("utf8");
  }
}

async function fetchOneQuote(apiSymbol) {
  const tencentCode = toTencentCode(apiSymbol);
  const url = `https://qt.gtimg.cn/q=${encodeURIComponent(tencentCode)}&_=${Date.now()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`行情请求失败 (${response.status})`);
  }

  const rawText = await readResponseText(response);
  const fields = parseTencentPayload(rawText);

  const lastPrice = toNumber(fields[3]);
  const previousClose = toNumber(fields[4]);

  if (lastPrice === null) {
    throw new Error("行情源未返回最新价");
  }

  return {
    apiSymbol,
    lastPrice,
    previousClose: previousClose ?? lastPrice,
    currency: guessCurrency(apiSymbol, fields),
    exchange: "",
    shortName: fields[1] || "",
    asOf: new Date().toISOString()
  };
}

async function lookupOneSecurityName(apiSymbol, options = {}) {
  const xueqiuCookie = String(options?.xueqiuCookie || "").trim();
  let xueqiuError = "";

  if (xueqiuCookie) {
    try {
      const shortName = await fetchOneXueqiuSecurityName(apiSymbol, xueqiuCookie);
      if (shortName) {
        return {
          ok: true,
          apiSymbol,
          shortName,
          source: "xueqiu"
        };
      }
    } catch (error) {
      xueqiuError = error.message;
    }
  }

  try {
    const payload = await fetchOneQuote(apiSymbol);
    return {
      ok: true,
      apiSymbol,
      shortName: String(payload.shortName || "").trim(),
      source: "tencent"
    };
  } catch (error) {
    return {
      ok: false,
      apiSymbol,
      error: xueqiuError ? `${xueqiuError}; ${error.message}` : error.message
    };
  }
}

async function lookupOneSecurityNameCached(apiSymbol, options = {}) {
  const cacheKey = buildSecurityNameCacheKey(apiSymbol, options);
  const now = Date.now();

  if (SECURITY_NAME_CACHE_TTL_MS > 0) {
    const cached = securityNameCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return { ...cached.result };
    }
  }

  if (securityNameInFlight.has(cacheKey)) {
    const shared = await securityNameInFlight.get(cacheKey);
    return { ...shared };
  }

  const task = lookupOneSecurityName(apiSymbol, options)
    .then((result) => {
      const ttlMs =
        result.ok && result.shortName ? SECURITY_NAME_CACHE_TTL_MS : SECURITY_NAME_FAILURE_CACHE_TTL_MS;

      if (ttlMs > 0) {
        securityNameCache.set(cacheKey, {
          result: { ...result },
          expiresAt: Date.now() + ttlMs
        });
      }
      return result;
    })
    .finally(() => {
      securityNameInFlight.delete(cacheKey);
    });

  securityNameInFlight.set(cacheKey, task);
  const result = await task;
  return { ...result };
}

async function mapLimit(items, limit, mapper) {
  const queue = [...items];
  const results = [];

  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (queue.length > 0) {
      const item = queue.shift();
      if (typeof item === "undefined") {
        continue;
      }
      const output = await mapper(item);
      results.push(output);
    }
  });

  await Promise.all(workers);
  return results;
}

async function refreshQuotes(apiSymbols) {
  const uniqueSymbols = [...new Set(apiSymbols.filter(Boolean))];

  const results = await mapLimit(uniqueSymbols, 4, async (apiSymbol) => {
    try {
      const payload = await fetchOneQuote(apiSymbol);
      return { ok: true, apiSymbol, payload };
    } catch (error) {
      return { ok: false, apiSymbol, error: error.message };
    }
  });

  const quotesBySymbol = {};
  const updated = [];
  const failed = [];

  for (const item of results) {
    if (item.ok) {
      quotesBySymbol[item.apiSymbol] = item.payload;
      updated.push(item.apiSymbol);
    } else {
      failed.push({
        apiSymbol: item.apiSymbol,
        reason: item.error
      });
    }
  }

  return {
    quotesBySymbol,
    updated,
    failed
  };
}

async function lookupSecurityNames(apiSymbols, options = {}) {
  const uniqueSymbols = [...new Set((apiSymbols || []).filter(Boolean))];
  if (uniqueSymbols.length === 0) {
    return {
      namesBySymbol: {},
      sourcesBySymbol: {},
      updated: [],
      failed: []
    };
  }

  const xueqiuCookie = String(options?.xueqiuCookie || "").trim();

  const results = await mapLimit(uniqueSymbols, 4, async (apiSymbol) => {
    return lookupOneSecurityNameCached(apiSymbol, { xueqiuCookie });
  });

  const namesBySymbol = {};
  const sourcesBySymbol = {};
  const updated = [];
  const failed = [];

  for (const item of results) {
    if (item.ok) {
      if (item.shortName) {
        namesBySymbol[item.apiSymbol] = item.shortName;
        sourcesBySymbol[item.apiSymbol] = String(item.source || "").trim() || "unknown";
      }
      updated.push(item.apiSymbol);
    } else {
      failed.push({
        apiSymbol: item.apiSymbol,
        reason: item.error
      });
    }
  }

  return {
    namesBySymbol,
    sourcesBySymbol,
    updated,
    failed
  };
}

module.exports = {
  refreshQuotes,
  lookupSecurityNames
};
