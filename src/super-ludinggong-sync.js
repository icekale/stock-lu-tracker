const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const { createHash } = require("node:crypto");
const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const { createWorker } = require("tesseract.js");
const { normalizeSecurityName, SYMBOL_NAME_OVERRIDES } = require("./symbols");

const XUEQIU_UID = "8790885129";
const WEIBO_UID = "3962719063";

const DEFAULT_PINNED_POST_URLS = ["https://xueqiu.com/8790885129/381996320"];
const DEFAULT_XUEQIU_TITLE_REGEX = "游戏仓\\s*20\\d{2}\\s*年\\s*\\d{1,2}\\s*月\\s*PS图";

const ACTION_KEYWORDS = [
  "加仓",
  "减仓",
  "新进仓",
  "新进",
  "新开仓",
  "清仓",
  "持仓不变",
  "增持",
  "减持",
  "买入",
  "卖出",
  "不变"
];

const SYMBOL_PATTERN = "(\\d{6}\\.(?:SH|SZ)|\\d{4,5}\\.HK|\\d{6}|\\d{4,5}|[A-Z]{1,6}(?:\\.[A-Z]{2})?)";
const SNAPSHOT_NAME_OVERRIDES = SYMBOL_NAME_OVERRIDES;
const SNAPSHOT_SYMBOL_OVERRIDES_BY_NAME = Object.freeze(
  Object.entries(SNAPSHOT_NAME_OVERRIDES).reduce((acc, [symbol, name]) => {
    const key = String(name || "").trim();
    if (!key) {
      return acc;
    }
    acc[key] = [...(acc[key] || []), symbol];
    return acc;
  }, {})
);
const QWEN_TABLE_SCHEMAS = Object.freeze([
  {
    key: "a-share",
    codePattern: /^\d{6}$/,
    columns: [
      { key: "symbol", aliases: ["证券代码"] },
      { key: "name", aliases: ["证券名称", "股票名称"] },
      { key: "balanceQty", aliases: ["股票余额"] },
      { key: "availableQty", aliases: ["可用余额"] },
      { key: "referenceCost", aliases: ["成本价"] },
      { key: "latestPrice", aliases: ["市价"] },
      { key: "marketValue", aliases: ["市值"] },
      { key: "floatingPnl", aliases: ["浮动盈亏"] },
      { key: "pnlPct", aliases: ["盈亏比例"] },
      { key: "marketName", aliases: ["交易市场"] }
    ]
  },
  {
    key: "hk",
    codePattern: /^\d{5}$/,
    columns: [
      { key: "name", aliases: ["证券名称", "股票名称"] },
      { key: "symbol", aliases: ["证券代码"] },
      { key: "holdingQty", aliases: ["可用股份", "股票余额"] },
      { key: "referenceCost", aliases: ["参考成本价", "成本价"] },
      { key: "referenceHoldingCost", aliases: ["参考持仓成本", "持仓成本"] },
      { key: "marketValue", aliases: ["最新市值", "市值"] },
      { key: "pnlPct", aliases: ["盈亏比例"] }
    ]
  }
]);

const DEFAULT_AUTO_TRACKING = {
  enabled: true,
  intervalMinutes: 180,
  xueqiuCookie: "",
  weiboCookie: "",
  maxPostsPerSource: 6,
  ocrEnabled: true,
  ocrProvider: "auto",
  ocrMaxImagesPerPost: 2,
  qwenApiKey: "",
  pinnedPostUrls: [...DEFAULT_PINNED_POST_URLS],
  xueqiuTitleRegex: DEFAULT_XUEQIU_TITLE_REGEX,
  backfillMaxPages: 36,
  backfillPageSize: 20,
  keywords: ["最新持仓", "调仓", "新开仓", "已清仓", "持仓", "组合"]
};

let ocrWorkerPromise;
const execFileAsync = promisify(execFile);
const QWEN_VL_OCR_MODEL = String(process.env.QWEN_VL_OCR_MODEL || "qwen-vl-ocr-latest").trim();
const QWEN_NATIVE_OCR_TIMEOUT_MS = clampNumber(process.env.QWEN_NATIVE_OCR_TIMEOUT_MS, 120000, 30000, 300000);
const OCR_CACHE_MAX_ITEMS = clampNumber(process.env.OCR_CACHE_MAX_ITEMS, 1200, 100, 10000);
const OCR_CACHE_TTL_MINUTES = clampNumber(process.env.OCR_CACHE_TTL_MINUTES, 24 * 60, 10, 14 * 24 * 60);
const OCR_CACHE_TTL_MS = OCR_CACHE_TTL_MINUTES * 60 * 1000;
const SNAPSHOT_PARSE_CONCURRENCY = clampNumber(process.env.SNAPSHOT_PARSE_CONCURRENCY, 3, 1, 6);
const OCR_DISK_CACHE_ENABLED = !["0", "false", "no", "off"].includes(
  String(process.env.OCR_DISK_CACHE_ENABLED || "true").trim().toLowerCase()
);
const OCR_CACHE_DIR = path.join(process.cwd(), "data", "ocr-cache");
const OCR_DISK_CACHE_PRUNE_INTERVAL_MS = 30 * 60 * 1000;
const ocrTextCache = new Map();
let localOcrQueue = Promise.resolve();
let ocrDiskCacheInitPromise = null;
let ocrDiskCacheWriteQueue = Promise.resolve();
let ocrDiskCachePrunePromise = null;
let lastOcrDiskCachePruneAt = 0;

function toDateIso(value) {
  if (!value && value !== 0) {
    return null;
  }

  const numeric = Number(value);
  if (Number.isFinite(numeric) && Math.abs(numeric) > 0) {
    const millis = Math.abs(numeric) < 1e12 ? numeric * 1000 : numeric;
    const fromNumber = new Date(millis);
    if (!Number.isNaN(fromNumber.getTime())) {
      return fromNumber.toISOString();
    }
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function parseNumeric(value) {
  if (!value && value !== 0) {
    return null;
  }

  let cleaned = String(value)
    .replace(/[，,]/g, "")
    .replace(/[−—]/g, "-")
    .replace(/\s+/g, "")
    .replace(/[^0-9+\-.]/g, "")
    .trim();

  if (!cleaned) {
    return null;
  }

  cleaned = cleaned.replace(/\.{2,}$/g, "");
  if (!cleaned) {
    return null;
  }

  const dotCount = (cleaned.match(/\./g) || []).length;
  if (dotCount > 1) {
    const lastDot = cleaned.lastIndexOf(".");
    cleaned = `${cleaned.slice(0, lastDot).replace(/\./g, "")}${cleaned.slice(lastDot)}`;
  }

  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseLeadingNumeric(value) {
  const matched = String(value || "").match(/^[+\-−]?\s*\d[\d,，]*(?:\.\d+)?/);
  if (!matched) {
    return null;
  }
  return parseNumeric(matched[0]);
}

function normalizeAshareSnapshotRow(row) {
  const output = { ...row };
  const balanceQty = parseNumeric(output.balanceQty);
  const availableQty = parseNumeric(output.availableQty);
  let holdingQty = parseNumeric(output.holdingQty);
  const referenceCost = parseNumeric(output.referenceCost);
  let latestPrice = parseNumeric(output.latestPrice);
  let marketValue = parseNumeric(output.marketValue);
  let floatingPnl = parseNumeric(output.floatingPnl);
  let pnlPct = parseNumeric(output.pnlPct);
  const trailingPct = parseLeadingNumeric(output.marketName);

  if (
    Number.isFinite(balanceQty) &&
    balanceQty > 0 &&
    Number.isFinite(availableQty) &&
    availableQty > balanceQty * 1.2
  ) {
    // OCR may glue "可用余额 + 成本价" into one number.
    holdingQty = balanceQty;
    output.availableQty = balanceQty;
  }

  if (!Number.isFinite(holdingQty) || holdingQty <= 0) {
    if (Number.isFinite(availableQty) && availableQty > 0) {
      holdingQty = availableQty;
    } else if (Number.isFinite(balanceQty) && balanceQty > 0) {
      holdingQty = balanceQty;
    }
  }
  output.holdingQty = holdingQty;
  output.balanceQty = Number.isFinite(balanceQty) ? balanceQty : output.balanceQty;
  output.availableQty = Number.isFinite(availableQty) ? availableQty : output.availableQty;
  output.changeQty = Number.isFinite(holdingQty) ? holdingQty : output.changeQty;

  if (!Number.isFinite(pnlPct) || Math.abs(pnlPct) > 500) {
    if (Number.isFinite(trailingPct)) {
      pnlPct = trailingPct;
    }
  }

  if (
    Number.isFinite(latestPrice) &&
    latestPrice > 10_000 &&
    Number.isFinite(holdingQty) &&
    holdingQty > 0 &&
    Number.isFinite(referenceCost) &&
    referenceCost > 0 &&
    referenceCost < 1_000
  ) {
    const oldLatestPrice = latestPrice;
    const oldMarketValue = marketValue;
    const oldFloatingPnl = floatingPnl;
    const impliedPrice = oldLatestPrice / holdingQty;
    const costGap = Math.abs(impliedPrice - referenceCost) / Math.max(referenceCost, 1);

    if (impliedPrice > 0 && impliedPrice < 10_000 && costGap < 2) {
      latestPrice = impliedPrice;
      marketValue = oldLatestPrice;

      if (Number.isFinite(oldMarketValue)) {
        if (Number.isFinite(oldFloatingPnl) && oldFloatingPnl >= 0 && oldFloatingPnl < 1_000) {
          floatingPnl = oldMarketValue + oldFloatingPnl / 1_000;
        } else {
          floatingPnl = oldMarketValue;
        }
      }
    }
  }

  const expectedValue =
    Number.isFinite(holdingQty) && holdingQty > 0 && Number.isFinite(latestPrice) && latestPrice > 0
      ? holdingQty * latestPrice
      : null;

  if (Number.isFinite(expectedValue) && expectedValue > 0) {
    const ratio = Number.isFinite(marketValue) && marketValue > 0 ? marketValue / expectedValue : 0;
    if (!Number.isFinite(ratio) || ratio < 0.2 || ratio > 5) {
      if (
        Number.isFinite(floatingPnl) &&
        floatingPnl > expectedValue * 0.5 &&
        floatingPnl < expectedValue * 1.5
      ) {
        marketValue = floatingPnl;
      } else {
        marketValue = expectedValue;
      }
    }

    const impliedPrice = marketValue / holdingQty;
    if (
      Number.isFinite(impliedPrice) &&
      impliedPrice > 0 &&
      Number.isFinite(latestPrice) &&
      latestPrice > 0 &&
      Math.round(latestPrice) === latestPrice
    ) {
      const gap = Math.abs(impliedPrice - latestPrice) / latestPrice;
      if (gap <= 0.2 && gap > 0.001) {
        latestPrice = impliedPrice;
      }
    }
  }

  output.latestPrice = Number.isFinite(latestPrice) ? Number(latestPrice.toFixed(3)) : output.latestPrice;
  output.marketValue = Number.isFinite(marketValue) ? marketValue : output.marketValue;
  output.floatingPnl = Number.isFinite(floatingPnl) ? floatingPnl : null;
  output.pnlPct = Number.isFinite(pnlPct) ? pnlPct : null;

  if (Number.isFinite(referenceCost) && Number.isFinite(holdingQty) && holdingQty > 0) {
    output.referenceHoldingCost = referenceCost * holdingQty;
  }

  if (Number.isFinite(trailingPct)) {
    output.marketName = sanitizeName(String(output.marketName || "").replace(/^[+\-−]?\s*\d[\d,，]*(?:\.\d+)?/, ""));
  }

  output.marketName = normalizeSnapshotMarketName(output.symbol, output.marketName);

  return output;
}

function normalizeReferenceHoldingCost(holdingQty, referenceCost, referenceHoldingCost) {
  const qty = parseNumeric(holdingQty);
  const cost = parseNumeric(referenceCost);
  const actual = parseNumeric(referenceHoldingCost);

  if (!Number.isFinite(qty) || qty <= 0 || !Number.isFinite(cost) || Math.abs(cost) < 0.000001) {
    return Number.isFinite(actual) ? actual : null;
  }

  const expected = qty * cost;
  if (!Number.isFinite(actual) || Math.abs(actual) < 1) {
    return expected;
  }

  const ratio = Math.abs(actual) / Math.max(Math.abs(expected), 1);
  if (!Number.isFinite(ratio) || ratio < 0.2 || ratio > 5) {
    return expected;
  }

  return actual;
}

function normalizeHkSnapshotRow(row) {
  const output = { ...row };
  const holdingQty = reconcileHoldingQty(
    output.holdingQty ?? output.availableQty ?? output.balanceQty ?? output.changeQty,
    output.referenceCost,
    output.referenceHoldingCost,
    output.symbol
  );
  let referenceCost = parseNumeric(output.referenceCost ?? output.latestCost);
  const referenceHoldingCost = normalizeReferenceHoldingCost(holdingQty, referenceCost, output.referenceHoldingCost);
  const marketValue = parseNumeric(output.marketValue);
  let latestPrice = parseNumeric(output.latestPrice);
  let floatingPnl = parseNumeric(output.floatingPnl);
  const pnlPct = parseNumeric(output.pnlPct);

  if (Number.isFinite(referenceHoldingCost) && Number.isFinite(holdingQty) && holdingQty > 0) {
    const inferredCost = referenceHoldingCost / holdingQty;
    if (!Number.isFinite(referenceCost) || Math.abs(referenceCost) > 100_000) {
      referenceCost = inferredCost;
    } else {
      const gap = Math.abs(referenceCost - inferredCost) / Math.max(Math.abs(inferredCost), 1);
      referenceCost = gap > 0.35 ? inferredCost : referenceCost;
    }
  }

  if (Number.isFinite(marketValue) && Number.isFinite(holdingQty) && holdingQty > 0) {
    const derivedLatestPrice = marketValue / holdingQty;
    if (!Number.isFinite(latestPrice) || latestPrice <= 0) {
      latestPrice = derivedLatestPrice;
    } else {
      const normalizedLatestPrice = Math.abs(latestPrice);
      const gap = Math.abs(normalizedLatestPrice - derivedLatestPrice) / Math.max(Math.abs(derivedLatestPrice), 1);
      latestPrice = gap > 0.2 ? derivedLatestPrice : normalizedLatestPrice;
    }
  } else if (Number.isFinite(latestPrice)) {
    latestPrice = Math.abs(latestPrice);
  }

  if (
    !Number.isFinite(floatingPnl) &&
    Number.isFinite(marketValue) &&
    Number.isFinite(referenceHoldingCost) &&
    referenceHoldingCost > 0
  ) {
    floatingPnl = marketValue - referenceHoldingCost;
  }

  output.holdingQty = Number.isFinite(holdingQty) ? Number(holdingQty) : output.holdingQty;
  output.balanceQty = Number.isFinite(holdingQty) ? Number(holdingQty) : output.balanceQty;
  output.availableQty = Number.isFinite(holdingQty) ? Number(holdingQty) : output.availableQty;
  output.changeQty = Number.isFinite(holdingQty) ? Number(holdingQty) : output.changeQty;
  output.referenceCost = Number.isFinite(referenceCost) ? Number(referenceCost) : output.referenceCost;
  output.latestCost = Number.isFinite(referenceCost) ? Number(referenceCost) : output.latestCost;
  output.referenceHoldingCost = Number.isFinite(referenceHoldingCost)
    ? Number(referenceHoldingCost)
    : output.referenceHoldingCost;
  output.latestPrice = Number.isFinite(latestPrice) ? Number(latestPrice) : output.latestPrice;
  output.marketValue = Number.isFinite(marketValue) ? Number(marketValue) : output.marketValue;
  output.floatingPnl = Number.isFinite(floatingPnl) ? Number(floatingPnl) : null;
  output.pnlPct = Number.isFinite(pnlPct) ? Number(pnlPct) : null;
  output.marketName = normalizeSnapshotMarketName(output.symbol, output.marketName);

  return output;
}

function rowQualityScore(row) {
  const qty = parseNumeric(row?.holdingQty ?? row?.availableQty ?? row?.balanceQty ?? row?.changeQty);
  const referenceCost = parseNumeric(row?.referenceCost ?? row?.latestCost);
  const referenceHoldingCost = parseNumeric(row?.referenceHoldingCost);
  const latestPrice = parseNumeric(row?.latestPrice);
  const marketValue = parseNumeric(row?.marketValue);
  let score = 0;

  if (Number.isFinite(qty) && qty > 0 && Number.isFinite(referenceCost) && Math.abs(referenceCost) > 0.000001 && Number.isFinite(referenceHoldingCost)) {
    const expectedCost = Math.abs(qty * referenceCost);
    const actualCost = Math.abs(referenceHoldingCost);
    score += (Math.abs(expectedCost - actualCost) / Math.max(actualCost, 1)) * 40;
  } else {
    score += 6;
  }

  if (Number.isFinite(qty) && qty > 0 && Number.isFinite(latestPrice) && latestPrice > 0 && Number.isFinite(marketValue) && marketValue > 0) {
    const expectedValue = qty * latestPrice;
    score += (Math.abs(expectedValue - marketValue) / Math.max(expectedValue, 1)) * 25;
  }

  if (!String(row?.name || "").trim()) {
    score += 2;
  }

  return score;
}

function mergeRowsBySymbol(rows) {
  const map = new Map();

  for (const row of dedupeRows(rows)) {
    const symbol = normalizeSnapshotSymbolByName(row?.symbol, row?.name) || String(row?.symbol || "").toUpperCase().trim();
    if (!symbol) {
      continue;
    }

    const normalizedRow = {
      ...row,
      symbol,
      name: normalizeSnapshotNameBySymbol(symbol, row?.name)
    };

    const existed = map.get(symbol);
    if (!existed) {
      map.set(symbol, normalizedRow);
      continue;
    }

    const currentScore = rowQualityScore(normalizedRow);
    const existedScore = rowQualityScore(existed);
    if (currentScore + 0.0001 < existedScore) {
      map.set(symbol, normalizedRow);
      continue;
    }

    if (Math.abs(currentScore - existedScore) <= 0.0001) {
      const currentMarketValue = parseNumeric(normalizedRow?.marketValue) || 0;
      const existedMarketValue = parseNumeric(existed?.marketValue) || 0;
      if (currentMarketValue > existedMarketValue) {
        map.set(symbol, normalizedRow);
      }
    }
  }

  return [...map.values()];
}

function inferHoldingQtyFromCost(referenceCost, referenceHoldingCost) {
  const cost = parseNumeric(referenceCost);
  const totalCost = parseNumeric(referenceHoldingCost);
  if (!Number.isFinite(cost) || !Number.isFinite(totalCost) || Math.abs(cost) < 0.000001 || Math.abs(totalCost) < 1) {
    return null;
  }

  const implied = Math.abs(totalCost / cost);
  if (!Number.isFinite(implied) || implied < 1) {
    return null;
  }

  const candidates = [Math.round(implied), Math.round(implied / 100) * 100]
    .filter((item) => Number.isFinite(item) && item > 0);

  let picked = null;
  let minGap = Infinity;
  for (const candidate of candidates) {
    const gap = Math.abs(candidate - implied) / implied;
    if (gap < minGap) {
      minGap = gap;
      picked = candidate;
    }
  }

  if (!Number.isFinite(picked) || minGap > 0.03) {
    return null;
  }

  return picked;
}

function alignQtyToMarketLot(symbol, rawQty) {
  const qty = parseNumeric(rawQty);
  if (!Number.isFinite(qty) || qty <= 0) {
    return qty;
  }

  const symbolText = String(symbol || "").trim().toUpperCase();
  const isHkSymbol = /^\d{4,5}(?:\.HK)?$/.test(symbolText);
  if (!isHkSymbol) {
    return qty;
  }

  const roundedBy100 = Math.round(qty / 100) * 100;
  if (!Number.isFinite(roundedBy100) || roundedBy100 <= 0) {
    return qty;
  }

  const diffRatio = Math.abs(qty - roundedBy100) / Math.max(qty, 1);
  return diffRatio <= 0.01 ? roundedBy100 : qty;
}

function reconcileHoldingQty(rawQty, referenceCost, referenceHoldingCost, symbol = "") {
  const parsedQty = parseNumeric(rawQty);
  const inferredQty = inferHoldingQtyFromCost(referenceCost, referenceHoldingCost);

  if (!Number.isFinite(parsedQty) || parsedQty <= 0) {
    return alignQtyToMarketLot(symbol, Number.isFinite(inferredQty) ? inferredQty : parsedQty);
  }

  if (!Number.isFinite(inferredQty) || inferredQty <= 0) {
    return alignQtyToMarketLot(symbol, parsedQty);
  }

  const diffRatio = Math.abs(parsedQty - inferredQty) / Math.max(inferredQty, 1);
  if (diffRatio >= 0.08) {
    return alignQtyToMarketLot(symbol, inferredQty);
  }

  return alignQtyToMarketLot(symbol, parsedQty);
}

function isSampleCookie(cookie) {
  const text = String(cookie || "").trim();
  if (!text) {
    return false;
  }

  const samples = ["abc123", "xyz987", "_2A25Labcde", "Hm_lvt_test"];
  return samples.some((item) => text.includes(item));
}

function getCookieState(cookie) {
  const text = String(cookie || "").trim();
  if (!text) {
    return "missing";
  }
  if (isSampleCookie(text)) {
    return "sample";
  }
  return "ok";
}

function hasUsableCookie(cookie) {
  return getCookieState(cookie) === "ok";
}

function cookieWarnText(sourceLabel, state) {
  if (state === "sample") {
    return `${sourceLabel} Cookie 是示例值，请替换成浏览器里复制的真实登录态`;
  }
  return `${sourceLabel} Cookie 未配置`;
}

function clampNumber(value, fallback, min, max) {
  const number = Number(value);
  const picked = Number.isFinite(number) ? number : fallback;
  return Math.max(min, Math.min(max, picked));
}

function normalizeOcrProvider(value) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "qwen" || text === "local") {
    return text;
  }
  return "auto";
}

function resolveQwenApiKey(config) {
  const preferred = String(config?.qwenApiKey || "").trim();
  if (preferred) {
    return preferred;
  }
  return String(process.env.DASHSCOPE_API_KEY || "").trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePinnedPostUrls(value) {
  const rawList = Array.isArray(value)
    ? value
    : String(value || "")
        .split(/[\n,]/)
        .map((item) => item.trim())
        .filter(Boolean);

  const urls = [];
  const seen = new Set();

  for (const raw of rawList) {
    const text = String(raw || "").trim();
    if (!text) {
      continue;
    }

    let parsed;
    try {
      parsed = new URL(text);
    } catch {
      continue;
    }

    const normalized = parsed.toString();
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    urls.push(normalized);
  }

  if (urls.length === 0) {
    return [...DEFAULT_PINNED_POST_URLS];
  }

  return urls.slice(0, 30);
}

function normalizeRegexInput(value, fallback) {
  const raw = String(value || "").trim();
  if (!raw) {
    return fallback;
  }

  try {
    void new RegExp(raw, "i");
    return raw;
  } catch {
    return fallback;
  }
}

function mergeAutoTrackingConfig(config) {
  const merged = {
    ...DEFAULT_AUTO_TRACKING,
    ...(config || {})
  };

  merged.intervalMinutes = clampNumber(merged.intervalMinutes, 180, 15, 24 * 60);
  merged.maxPostsPerSource = clampNumber(merged.maxPostsPerSource, 6, 1, 50);
  merged.ocrProvider = normalizeOcrProvider(merged.ocrProvider);
  merged.ocrMaxImagesPerPost = clampNumber(merged.ocrMaxImagesPerPost, 2, 1, 6);
  merged.qwenApiKey = String(merged.qwenApiKey || "").trim();
  merged.backfillMaxPages = clampNumber(merged.backfillMaxPages, 36, 1, 120);
  merged.backfillPageSize = clampNumber(merged.backfillPageSize, 20, 5, 50);
  merged.pinnedPostUrls = normalizePinnedPostUrls(merged.pinnedPostUrls);
  merged.xueqiuTitleRegex = normalizeRegexInput(merged.xueqiuTitleRegex, DEFAULT_XUEQIU_TITLE_REGEX);
  merged.keywords = Array.isArray(merged.keywords)
    ? merged.keywords.map((item) => String(item || "").trim()).filter(Boolean)
    : [...DEFAULT_AUTO_TRACKING.keywords];

  return merged;
}

function ensureAutoTrackingState(store) {
  const current = store.autoTracking || {};

  store.autoTracking = {
    config: mergeAutoTrackingConfig(current.config),
    runtime: {
      lastRunAt: current.runtime?.lastRunAt || null,
      lastSuccessAt: current.runtime?.lastSuccessAt || null,
      lastError: current.runtime?.lastError || null,
      nextRunAt: current.runtime?.nextRunAt || null,
      totalImportedSnapshots: Number(current.runtime?.totalImportedSnapshots) || 0,
      totalImportedTrades: Number(current.runtime?.totalImportedTrades) || 0
    },
    processedPostIds: Array.isArray(current.processedPostIds) ? current.processedPostIds : [],
    importedTradeKeys: Array.isArray(current.importedTradeKeys) ? current.importedTradeKeys : [],
    logs: Array.isArray(current.logs) ? current.logs : [],
    latestSnapshot: current.latestSnapshot || null
  };

  return store.autoTracking;
}

function stripHtml(html) {
  const raw = String(html || "");
  return raw
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<\/div>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'")
    .replace(/\r/g, "\n")
    .replace(/\t/g, " ")
    .replace(/[|｜]/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function safeJsonParse(text) {
  if (typeof text !== "string" || !text.trim()) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function shortText(value, max = 160) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.length <= max ? text : `${text.slice(0, max)}...`;
}

function isLikelySymbol(token) {
  const value = String(token || "").toUpperCase().trim();
  if (!value) {
    return false;
  }

  if (/^\d{6}\.(SH|SZ)$/.test(value)) {
    return true;
  }

  if (/^\d{4,5}\.HK$/.test(value)) {
    return true;
  }

  if (/^[A-Z]{1,6}(\.[A-Z]{2})?$/.test(value)) {
    return true;
  }

  if (/^\d{6}$/.test(value) || /^\d{4,5}$/.test(value)) {
    return true;
  }

  return false;
}

function normalizeAction(actionRaw, changeQty) {
  const action = String(actionRaw || "").trim();

  if (action.includes("加") || action.includes("新进") || action.includes("新开") || action.includes("增")) {
    return "BUY";
  }

  if (action.includes("减") || action.includes("清仓") || action.includes("卖")) {
    return "SELL";
  }

  if (action.includes("持仓不变") || action.includes("不变")) {
    return "HOLD";
  }

  if (Number.isFinite(changeQty)) {
    if (changeQty > 0) {
      return "BUY";
    }
    if (changeQty < 0) {
      return "SELL";
    }
  }

  return "UNKNOWN";
}

function sanitizeName(value) {
  return String(value || "")
    .replace(/[\s|｜]+/g, " ")
    .replace(/^[-–—]+|[-–—]+$/g, "")
    .trim();
}

function normalizeSnapshotNameBySymbol(symbol, value) {
  const normalizedSymbol = normalizeSnapshotSymbol(symbol);
  if (normalizedSymbol && SNAPSHOT_NAME_OVERRIDES[normalizedSymbol]) {
    return SNAPSHOT_NAME_OVERRIDES[normalizedSymbol];
  }
  return normalizeSecurityName(normalizedSymbol || symbol, sanitizeName(value));
}

function inferCnMarketName(symbol) {
  return String(symbol || "").startsWith("6") || String(symbol || "").startsWith("9") ? "上海A股" : "深圳A股";
}

function normalizeSnapshotMarketName(symbol, value) {
  const text = sanitizeName(value);

  if (/港股/.test(text)) {
    return "港股通";
  }
  if (/上海A股|沪A|上海/.test(text)) {
    return "上海A股";
  }
  if (/深圳A股|深A|深圳/.test(text)) {
    return "深圳A股";
  }

  const normalizedSymbol = normalizeSnapshotSymbol(symbol);
  if (normalizedSymbol && /^\d{6}$/.test(normalizedSymbol)) {
    return inferCnMarketName(normalizedSymbol);
  }
  if (normalizedSymbol && /^\d{4,5}$/.test(normalizedSymbol)) {
    return "港股通";
  }

  return text;
}

function isTruncatedCellText(value) {
  return /(?:\.\.\.|…)/.test(String(value || ""));
}

function parseOcrNumericCell(value) {
  if (isTruncatedCellText(value)) {
    return null;
  }
  return parseNumeric(value);
}

function computeMedian(values) {
  const list = Array.isArray(values)
    ? values.map((item) => Number(item)).filter((item) => Number.isFinite(item)).sort((a, b) => a - b)
    : [];

  if (list.length === 0) {
    return null;
  }

  const middle = Math.floor(list.length / 2);
  if (list.length % 2 === 1) {
    return list[middle];
  }

  return (list[middle - 1] + list[middle]) / 2;
}

function buildRowCandidate(input) {
  const symbol = String(input.symbol || "").toUpperCase().trim();
  const name = sanitizeName(input.name || "");
  const actionLabel = String(input.actionLabel || "").trim();
  const changeQty = parseNumeric(input.changeQty);
  const latestCost = parseNumeric(input.latestCost);

  if (!isLikelySymbol(symbol) || symbol.includes("CASH")) {
    return null;
  }

  if (!Number.isFinite(latestCost) || latestCost <= 0) {
    return null;
  }

  const action = normalizeAction(actionLabel, changeQty);
  if (!Number.isFinite(changeQty) && action !== "HOLD") {
    return null;
  }

  if (
    name.includes("代码") ||
    name.includes("股票名称") ||
    name.includes("操作记录") ||
    name.includes("变动股数") ||
    name.includes("最新成本")
  ) {
    return null;
  }

  return {
    symbol,
    name,
    actionLabel,
    action,
    changeQty: Number.isFinite(changeQty) ? changeQty : 0,
    latestCost: Math.abs(latestCost)
  };
}

function dedupeRows(rows) {
  const deduped = [];
  const seen = new Set();

  for (const row of rows) {
    const key = `${row.symbol}|${row.action}|${row.changeQty}|${row.latestCost}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(row);
  }

  return deduped;
}

function normalizeOcrLine(line) {
  return String(line || "")
    .replace(/[|｜]/g, " ")
    .replace(/[，,]/g, " ")
    .replace(/[。；;]/g, " ")
    .replace(/(\d)\s*\.\s*(\d)/g, "$1.$2")
    .replace(/\b(\d{1,3})\s+(\d{3})(?=\s+\d{4,}(?:\.\d+)?\b)/g, "$1.$2")
    .replace(/\b(\d{5,})\s+(\d{1,3})(?=\s+[+\-−]?\d{1,3}\.\d{1,2}\b)/g, "$1.$2")
    .replace(/([+\-−])\s+(\d)/g, "$1$2")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function normalizeCodeToken(rawToken) {
  const compact = String(rawToken || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  if (!compact) {
    return null;
  }

  const mapped = compact
    .replace(/[OQDU]/g, "0")
    .replace(/[IL]/g, "1")
    .replace(/Z/g, "2")
    .replace(/S/g, "5")
    .replace(/G/g, "6")
    .replace(/T/g, "7")
    .replace(/B/g, "8");

  if (/^\d{6}$/.test(mapped) || /^\d{4,5}$/.test(mapped)) {
    return mapped;
  }

  return null;
}

function extractRowsFromText(inputText) {
  const rows = [];
  const text = stripHtml(inputText);
  const symbolRegex = new RegExp(SYMBOL_PATTERN, "i");
  const actionRegex = new RegExp(`(${ACTION_KEYWORDS.join("|")})`);
  const lines = text
    .split(/\n+/)
    .map((line) => line.replace(/\s{2,}/g, " ").trim())
    .filter(Boolean);
  const normalizedLines = lines.map((line) => normalizeOcrLine(line)).filter(Boolean);

  const holdingsLineRegex =
    /^(.+?)\s+(\d{4,5}(?:\.HK)?)\s+(-?\d[\d,]*)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)$/i;
  const aShareLineRegex =
    /^(\d{6})\s+(.+?)\s+(-?\d[\d,]*)\s+(-?\d[\d,]*)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*(.*)$/i;

  for (const line of normalizedLines) {
    if (
      line.includes("证券名称") ||
      line.includes("证券代码") ||
      line.includes("可用股份") ||
      line.includes("参考成本价") ||
      line.includes("参考持仓成本") ||
      line.includes("最新市值") ||
      line.includes("盈亏比例")
    ) {
      continue;
    }

    const matched = line.match(holdingsLineRegex);
    if (!matched) {
      continue;
    }

    const name = sanitizeName(matched[1]);
    const symbol = String(matched[2] || "").toUpperCase();
    const holdingQtyRaw = parseNumeric(matched[3]);
    const referenceCost = parseNumeric(matched[4]);
    const referenceHoldingCost = parseNumeric(matched[5]);
    const marketValue = parseNumeric(matched[6]);
    const pnlPct = parseNumeric(matched[7]);
    const holdingQty = reconcileHoldingQty(holdingQtyRaw, referenceCost, referenceHoldingCost, symbol);

    if (
      !isLikelySymbol(symbol) ||
      !Number.isFinite(holdingQty) ||
      !Number.isFinite(referenceCost) ||
      !Number.isFinite(referenceHoldingCost) ||
      !Number.isFinite(marketValue)
    ) {
      continue;
    }

    rows.push(
      normalizeHkSnapshotRow({
        symbol,
        name,
        actionLabel: "持仓快照",
        action: "HOLD",
        changeQty: holdingQty,
        latestCost: referenceCost,
        holdingQty,
        referenceCost,
        referenceHoldingCost,
        marketValue,
        pnlPct: Number.isFinite(pnlPct) ? pnlPct : null,
        marketName: ""
      })
    );
  }

  for (const line of normalizedLines) {
    if (
      line.includes("证券代码") ||
      line.includes("证券名称") ||
      line.includes("股票余额") ||
      line.includes("可用余额") ||
      line.includes("成本价") ||
      line.includes("市价") ||
      line.includes("市值") ||
      line.includes("浮动盈亏") ||
      line.includes("盈亏比例") ||
      line.includes("交易市场")
    ) {
      continue;
    }

    const matched = line.match(aShareLineRegex);
    if (!matched) {
      continue;
    }

    const symbol = String(matched[1] || "").toUpperCase();
    const name = sanitizeName(matched[2]);
    const balanceQty = parseNumeric(matched[3]);
    const availableQty = parseNumeric(matched[4]);
    const referenceCost = parseNumeric(matched[5]);
    const latestPrice = parseNumeric(matched[6]);
    const marketValue = parseNumeric(matched[7]);
    const floatingPnl = parseNumeric(matched[8]);
    const pnlPct = parseNumeric(matched[9]);
    const marketName = sanitizeName(matched[10] || "");

    if (
      !isLikelySymbol(symbol) ||
      !Number.isFinite(balanceQty) ||
      !Number.isFinite(referenceCost) ||
      !Number.isFinite(latestPrice) ||
      !Number.isFinite(marketValue)
    ) {
      continue;
    }

    const holdingQty = Number.isFinite(availableQty) ? availableQty : balanceQty;
    const referenceHoldingCost = Number.isFinite(referenceCost) ? referenceCost * holdingQty : null;

    rows.push(
      normalizeAshareSnapshotRow({
        symbol,
        name,
        actionLabel: "持仓快照",
        action: "HOLD",
        changeQty: holdingQty,
        latestCost: referenceCost,
        holdingQty,
        availableQty,
        balanceQty,
        referenceCost,
        latestPrice,
        referenceHoldingCost,
        marketValue,
        floatingPnl: Number.isFinite(floatingPnl) ? floatingPnl : null,
        pnlPct: Number.isFinite(pnlPct) ? pnlPct : null,
        marketName
      })
    );
  }

  for (const line of normalizedLines) {
    const symbolMatch = line.match(symbolRegex);
    if (!symbolMatch) {
      continue;
    }

    const symbol = symbolMatch[1].toUpperCase();
    const symbolIndex = line.indexOf(symbolMatch[0]);
    const tail = line.slice(symbolIndex + symbolMatch[0].length).trim();

    const actionMatch = tail.match(actionRegex);
    if (!actionMatch) {
      continue;
    }
    const actionLabel = actionMatch[1];

    const numericMatches = [...tail.matchAll(/[+\-−]?\s*\d[\d,，]*(?:\.\d+)?/g)];
    if (numericMatches.length === 0) {
      continue;
    }

    const latestCost = parseNumeric(numericMatches[numericMatches.length - 1][0]);
    let changeQty = parseNumeric(numericMatches[0][0]);

    if (numericMatches.length >= 2) {
      const firstNum = parseNumeric(numericMatches[0][0]);
      const lastNum = parseNumeric(numericMatches[numericMatches.length - 1][0]);
      if (Number.isFinite(firstNum) && Number.isFinite(lastNum) && Math.abs(firstNum) < 1000 && Math.abs(lastNum) > 1000) {
        changeQty = firstNum;
      }
    }

    const nameEnd = actionMatch ? tail.indexOf(actionMatch[1]) : tail.search(/[+\-−]?\s*\d[\d,，]*/);
    const name = nameEnd > 0 ? tail.slice(0, nameEnd) : "";
    const row = buildRowCandidate({
      symbol,
      name,
      actionLabel,
      changeQty,
      latestCost
    });

    if (row) {
      rows.push(row);
    }
  }

  const compact = text.replace(/\s+/g, " ").trim();
  const pattern = new RegExp(
    `${SYMBOL_PATTERN}\\s*([\\u4e00-\\u9fa5A-Za-z0-9*()（）\\-]{0,24})\\s*(${ACTION_KEYWORDS.join("|")})\\s*([+\\-−]?\\s*\\d[\\d,，]*)\\s*([+\\-−]?\\d+(?:\\.\\d+)?)`,
    "gi"
  );

  for (const match of compact.matchAll(pattern)) {
    const row = buildRowCandidate({
      symbol: match[1],
      name: match[2],
      actionLabel: match[3],
      changeQty: match[4],
      latestCost: match[5]
    });

    if (row) {
      rows.push(row);
    }
  }

  for (const line of normalizedLines) {
    if (
      line.includes("证券代码") ||
      line.includes("证券名称") ||
      line.includes("股票余额") ||
      line.includes("可用余额") ||
      line.includes("成本价") ||
      line.includes("市价") ||
      line.includes("市值") ||
      line.includes("浮动盈亏") ||
      line.includes("盈亏比例") ||
      line.includes("交易市场")
    ) {
      continue;
    }

    const tokens = line.split(/\s+/).filter(Boolean);
    if (tokens.length < 7) {
      continue;
    }

    let codeIndex = -1;
    let normalizedCode = null;
    for (let i = 0; i < tokens.length; i += 1) {
      const normalized = normalizeCodeToken(tokens[i]);
      if (normalized && /^\d{6}$/.test(normalized)) {
        codeIndex = i;
        normalizedCode = normalized;
        break;
      }
    }

    if (codeIndex < 0 || !normalizedCode) {
      continue;
    }

    const tailTokens = tokens.slice(codeIndex + 1);
    const firstNumericIndex = tailTokens.findIndex((token) => Number.isFinite(parseNumeric(token)));
    if (firstNumericIndex < 0) {
      continue;
    }

    const name = sanitizeName(tailTokens.slice(0, firstNumericIndex).join(""));
    const numberTokens = tailTokens.slice(firstNumericIndex);
    const numbers = numberTokens.map((item) => parseNumeric(item)).filter((item) => Number.isFinite(item));

    if (!name || numbers.length < 5) {
      continue;
    }

    const balanceQty = numbers[0];
    const availableQty = numbers[1];
    const referenceCost = numbers[2];
    const latestPrice = numbers[3];
    const marketValue = numbers[4];
    const floatingPnl = numbers.length >= 6 ? numbers[5] : null;
    const pnlPct = numbers.length >= 7 ? numbers[6] : null;

    if (
      !Number.isFinite(balanceQty) ||
      !Number.isFinite(referenceCost) ||
      !Number.isFinite(latestPrice) ||
      !Number.isFinite(marketValue)
    ) {
      continue;
    }

    const holdingQty = Number.isFinite(availableQty) ? availableQty : balanceQty;

    rows.push(
      normalizeAshareSnapshotRow({
        symbol: normalizedCode,
        name,
        actionLabel: "持仓快照",
        action: "HOLD",
        changeQty: holdingQty,
        latestCost: referenceCost,
        holdingQty,
        availableQty,
        balanceQty,
        referenceCost,
        latestPrice,
        referenceHoldingCost: Number.isFinite(referenceCost) ? referenceCost * holdingQty : null,
        marketValue,
        floatingPnl: Number.isFinite(floatingPnl) ? floatingPnl : null,
        pnlPct: Number.isFinite(pnlPct) ? pnlPct : null,
        marketName: ""
      })
    );
  }

  return mergeRowsBySymbol(rows);
}

function extractImageUrlsFromObject(input) {
  const urls = [];
  const seen = new Set();

  function pushUrl(url) {
    const text = String(url || "").trim();
    if (!/^https?:\/\//i.test(text)) {
      return;
    }
    if (seen.has(text)) {
      return;
    }
    seen.add(text);
    urls.push(text);
  }

  function walk(value) {
    if (!value) {
      return;
    }

    if (typeof value === "string") {
      for (const match of value.matchAll(/https?:\/\/[^\s'"<>]+\.(?:png|jpg|jpeg|webp)/gi)) {
        pushUrl(match[0]);
      }
      for (const match of value.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)) {
        pushUrl(match[1]);
      }
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        walk(item);
      }
      return;
    }

    if (typeof value === "object") {
      for (const [key, item] of Object.entries(value)) {
        const lowerKey = key.toLowerCase();
        if (lowerKey.includes("url") && typeof item === "string") {
          pushUrl(item);
        } else {
          walk(item);
        }
      }
    }
  }

  walk(input);

  return urls;
}

function normalizeImageUrlList(rawList) {
  const urls = [];
  const seen = new Set();

  const push = (value) => {
    const text = String(value || "").trim();
    if (!/^https?:\/\//i.test(text)) {
      return;
    }
    if (!/\.(png|jpg|jpeg|webp)(\?|$)/i.test(text)) {
      return;
    }
    if (seen.has(text)) {
      return;
    }
    seen.add(text);
    urls.push(text);
  };

  if (Array.isArray(rawList)) {
    for (const item of rawList) {
      push(item);
    }
  }

  return urls;
}

function extractXueqiuImageUrls(raw) {
  const directUrls = [];

  if (typeof raw?.firstImg === "string") {
    directUrls.push(raw.firstImg);
  }

  if (typeof raw?.pic === "string") {
    directUrls.push(
      ...raw.pic
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    );
  }

  if (Array.isArray(raw?.image_info_list)) {
    for (const item of raw.image_info_list) {
      if (typeof item === "string") {
        directUrls.push(item);
      } else if (item && typeof item === "object") {
        directUrls.push(
          item.origin_url,
          item.large_url,
          item.url,
          item.pic_url
        );
      }
    }
  }

  if (typeof raw?.cover_pic === "string") {
    directUrls.push(raw.cover_pic);
  }

  const normalizedDirect = normalizeImageUrlList(directUrls);
  if (normalizedDirect.length > 0) {
    return normalizedDirect;
  }

  return extractImageUrlsFromObject(raw);
}

function extractWeiboImageUrls(raw) {
  const directUrls = [];

  if (Array.isArray(raw?.pics)) {
    for (const item of raw.pics) {
      if (typeof item === "string") {
        directUrls.push(item);
      } else if (item && typeof item === "object") {
        directUrls.push(item.large?.url, item.url, item.pic_big?.url, item.bmiddle?.url, item.thumbnail?.url);
      }
    }
  }

  if (raw?.pic_infos && typeof raw.pic_infos === "object") {
    for (const value of Object.values(raw.pic_infos)) {
      if (value && typeof value === "object") {
        directUrls.push(value.largest?.url, value.large?.url, value.bmiddle?.url, value.thumbnail?.url);
      }
    }
  }

  const normalizedDirect = normalizeImageUrlList(directUrls);
  if (normalizedDirect.length > 0) {
    return normalizedDirect;
  }

  return extractImageUrlsFromObject(raw);
}

function buildXueqiuHeaders(cookie, referer) {
  return {
    "User-Agent": "Mozilla/5.0",
    Accept: "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    Referer: referer || `https://xueqiu.com/u/${XUEQIU_UID}`,
    Origin: "https://xueqiu.com",
    "X-Requested-With": "XMLHttpRequest",
    Cookie: cookie
  };
}

function buildWeiboHeaders(cookie, referer) {
  return {
    "User-Agent": "Mozilla/5.0",
    Accept: "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    Referer: referer || `https://weibo.com/u/${WEIBO_UID}`,
    Origin: "https://weibo.com",
    "X-Requested-With": "XMLHttpRequest",
    Cookie: cookie
  };
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 25000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

async function requestJson(url, options = {}, sourceLabel = "请求") {
  const response = await fetchWithTimeout(url, options);
  const text = await response.text();
  const data = safeJsonParse(text);

  if (!response.ok) {
    const detail = data?.error_description || data?.error || shortText(text) || "未知错误";
    throw new Error(`${sourceLabel}失败 (${response.status}): ${detail}`);
  }

  if (!data) {
    throw new Error(`${sourceLabel}返回非 JSON（可能被风控拦截）`);
  }

  if (data?.error_code) {
    throw new Error(`${sourceLabel}返回错误: ${data.error_description || data.error_code}`);
  }

  return data;
}

function buildXueqiuTitleRegex(config) {
  const text = String(config?.xueqiuTitleRegex || DEFAULT_XUEQIU_TITLE_REGEX).trim();
  try {
    return new RegExp(text, "i");
  } catch {
    return new RegExp(DEFAULT_XUEQIU_TITLE_REGEX, "i");
  }
}

function pickXueqiuStatusPayload(data) {
  if (data?.status && typeof data.status === "object") {
    return data.status;
  }
  if (data?.data?.status && typeof data.data.status === "object") {
    return data.data.status;
  }
  if (data?.id || data?.status_id || data?.description || data?.text) {
    return data;
  }
  return null;
}

function normalizeXueqiuPost(raw, options = {}) {
  const id = String(options.postId || raw?.id || raw?.status_id || raw?.created_at || raw?.title || Math.random());
  const created = raw?.created_at || raw?.time_before || raw?.updated_at;
  const postedAt = toDateIso(created) || new Date().toISOString();
  const title = String(raw?.title || raw?.description_title || "").trim();
  const text = raw?.description || raw?.text || raw?.title || raw?.description_text || "";
  const images = extractXueqiuImageUrls(raw);

  return {
    source: "xueqiu",
    postId: `xq:${id}`,
    title,
    text,
    postedAt,
    images,
    link: options.link || `https://xueqiu.com/${XUEQIU_UID}/${id}`,
    fromPinned: Boolean(options.fromPinned),
    raw
  };
}

function normalizeWeiboPost(raw, options = {}) {
  const id = String(options.postId || raw?.idstr || raw?.id || raw?.mid || raw?.mblogid || Math.random());
  const postedAt = toDateIso(raw?.created_at) || new Date().toISOString();
  const title = String(raw?.title || "").trim();
  const text = raw?.text_raw || raw?.text || "";
  const images = extractWeiboImageUrls(raw);

  return {
    source: "weibo",
    postId: `wb:${id}`,
    title,
    text,
    postedAt,
    images,
    link: options.link || `https://weibo.com/u/${WEIBO_UID}`,
    fromPinned: Boolean(options.fromPinned),
    raw
  };
}

function isLikelyHoldingPost(post, keywords) {
  const text = `${String(post.title || "")}\n${String(post.text || "")}`;
  return keywords.some((kw) => text.includes(kw));
}

function isXueqiuTargetTitlePost(post, titleRegex) {
  if (!post || post.source !== "xueqiu") {
    return false;
  }
  const title = String(post.title || post.raw?.title || "");
  const text = String(post.text || "");
  return titleRegex.test(title) || titleRegex.test(text);
}

function extractXueqiuPostIdFromUrl(rawUrl) {
  if (!rawUrl) {
    return null;
  }

  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return null;
  }

  const queryId = parsed.searchParams.get("id") || parsed.searchParams.get("status_id");
  if (queryId && /^\d{6,}$/.test(queryId)) {
    return queryId;
  }

  const parts = parsed.pathname.split("/").filter(Boolean);
  for (let i = parts.length - 1; i >= 0; i -= 1) {
    const token = parts[i];
    if (/^\d{6,}$/.test(token)) {
      return token;
    }
  }

  return null;
}

function extractWeiboPostIdFromUrl(rawUrl) {
  if (!rawUrl) {
    return null;
  }

  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return null;
  }

  const queryId = parsed.searchParams.get("id") || parsed.searchParams.get("mid");
  if (queryId) {
    return queryId;
  }

  const parts = parsed.pathname.split("/").filter(Boolean);
  if (parts.length === 0) {
    return null;
  }

  if (parts[0].toLowerCase() === "u") {
    return null;
  }

  const last = parts[parts.length - 1];
  if (/^[A-Za-z0-9]{6,}$/.test(last)) {
    return last;
  }

  return null;
}

function detectSourceByUrl(rawUrl) {
  try {
    const hostname = new URL(rawUrl).hostname.toLowerCase();
    if (hostname.includes("xueqiu.com")) {
      return "xueqiu";
    }
    if (hostname.includes("weibo.com") || hostname.includes("weibo.cn")) {
      return "weibo";
    }
    return "unknown";
  } catch {
    return "unknown";
  }
}

async function fetchXueqiuPostById(postId, config, postUrl) {
  const headers = buildXueqiuHeaders(config.xueqiuCookie, postUrl || `https://xueqiu.com/u/${XUEQIU_UID}`);
  const endpoints = [
    `https://xueqiu.com/statuses/original/show.json?id=${encodeURIComponent(postId)}`,
    `https://xueqiu.com/statuses/show.json?id=${encodeURIComponent(postId)}`
  ];

  const errors = [];
  for (const endpoint of endpoints) {
    try {
      const data = await requestJson(endpoint, { headers }, "雪球帖子详情");
      const payload = pickXueqiuStatusPayload(data);
      if (!payload) {
        throw new Error("返回数据里没有帖子详情");
      }
      return normalizeXueqiuPost(payload, {
        postId,
        link: postUrl,
        fromPinned: true
      });
    } catch (error) {
      errors.push(error.message);
    }
  }

  try {
    const maxPages = 8;
    const pageSize = Math.max(10, Math.min(50, Number(config.backfillPageSize) || 20));
    for (let page = 1; page <= maxPages; page += 1) {
      const list = await fetchXueqiuTimelinePage(config, page, pageSize);
      const matched = list.find((item) => String(item?.id || item?.status_id || "") === String(postId));
      if (matched) {
        return normalizeXueqiuPost(matched, {
          postId,
          link: postUrl,
          fromPinned: true
        });
      }
      if (!Array.isArray(list) || list.length < pageSize) {
        break;
      }
      await sleep(220);
    }
    errors.push("雪球详情接口被风控，且在时间线中未找到指定帖子");
  } catch (error) {
    errors.push(`时间线回退失败: ${error.message}`);
  }

  throw new Error(errors.join(" | "));
}

async function fetchWeiboPostById(postId, config, postUrl) {
  const headers = buildWeiboHeaders(config.weiboCookie, postUrl || `https://weibo.com/u/${WEIBO_UID}`);
  const endpoint = `https://weibo.com/ajax/statuses/show?id=${encodeURIComponent(postId)}`;
  const data = await requestJson(endpoint, { headers }, "微博帖子详情");
  const payload = data?.data && typeof data.data === "object" ? data.data : data;
  return normalizeWeiboPost(payload, {
    postId,
    link: postUrl,
    fromPinned: true
  });
}

async function fetchPinnedPosts(config, addLog) {
  const posts = [];
  const urls = Array.isArray(config.pinnedPostUrls) ? config.pinnedPostUrls : [];

  for (const rawUrl of urls) {
    const url = String(rawUrl || "").trim();
    if (!url) {
      continue;
    }

    const source = detectSourceByUrl(url);
    if (source === "xueqiu") {
      const xqCookieState = getCookieState(config.xueqiuCookie);
      if (xqCookieState !== "ok") {
        addLog("warn", `置顶链接跳过（${cookieWarnText("雪球", xqCookieState)}）: ${url}`);
        continue;
      }

      const postId = extractXueqiuPostIdFromUrl(url);
      if (!postId) {
        addLog("warn", `置顶链接跳过（无法识别雪球帖子ID）: ${url}`);
        continue;
      }

      try {
        const post = await fetchXueqiuPostById(postId, config, url);
        posts.push(post);
        addLog("info", `置顶链接抓取成功（雪球）: ${postId}`);
      } catch (error) {
        addLog("error", `置顶链接抓取失败（雪球）: ${postId} | ${error.message}`);
      }
      continue;
    }

    if (source === "weibo") {
      const wbCookieState = getCookieState(config.weiboCookie);
      if (wbCookieState !== "ok") {
        addLog("warn", `置顶链接跳过（${cookieWarnText("微博", wbCookieState)}）: ${url}`);
        continue;
      }

      const postId = extractWeiboPostIdFromUrl(url);
      if (!postId) {
        addLog("warn", `置顶链接跳过（无法识别微博帖子ID）: ${url}`);
        continue;
      }

      try {
        const post = await fetchWeiboPostById(postId, config, url);
        posts.push(post);
        addLog("info", `置顶链接抓取成功（微博）: ${postId}`);
      } catch (error) {
        addLog("error", `置顶链接抓取失败（微博）: ${postId} | ${error.message}`);
      }
      continue;
    }

    addLog("warn", `置顶链接跳过（暂不支持的站点）: ${url}`);
  }

  return posts;
}

function extractXueqiuTimelineList(data) {
  if (Array.isArray(data?.list)) {
    return data.list;
  }
  if (Array.isArray(data?.statuses)) {
    return data.statuses;
  }
  if (Array.isArray(data?.data?.list)) {
    return data.data.list;
  }
  return [];
}

async function fetchXueqiuTimelinePage(config, page = 1, pageSize = 20) {
  if (!hasUsableCookie(config.xueqiuCookie)) {
    return [];
  }

  const headers = buildXueqiuHeaders(config.xueqiuCookie, `https://xueqiu.com/u/${XUEQIU_UID}`);
  const endpoints = [
    `https://xueqiu.com/statuses/user_timeline.json?user_id=${XUEQIU_UID}&page=${page}&count=${pageSize}`,
    `https://xueqiu.com/statuses/original/user_timeline.json?user_id=${XUEQIU_UID}&page=${page}&count=${pageSize}`
  ];

  const errors = [];
  for (const endpoint of endpoints) {
    try {
      const data = await requestJson(endpoint, { headers }, "雪球时间线");
      return extractXueqiuTimelineList(data);
    } catch (error) {
      errors.push(error.message);
    }
  }

  throw new Error(errors.join(" | "));
}

async function fetchXueqiuPosts(config, options = {}) {
  const pageFrom = clampNumber(options.pageFrom, 1, 1, 999);
  const pageTo = clampNumber(options.pageTo, pageFrom, pageFrom, 999);
  const pageSize = clampNumber(options.pageSize, config.maxPostsPerSource, 1, 50);
  const maxTotal = Number.isFinite(Number(options.maxTotal))
    ? clampNumber(options.maxTotal, pageSize, 1, 5000)
    : null;

  const results = [];
  for (let page = pageFrom; page <= pageTo; page += 1) {
    const list = await fetchXueqiuTimelinePage(config, page, pageSize);
    if (!Array.isArray(list) || list.length === 0) {
      break;
    }

    results.push(...list.map((item) => normalizeXueqiuPost(item)));

    if (maxTotal && results.length >= maxTotal) {
      break;
    }

    if (page < pageTo) {
      await sleep(240);
    }
  }

  return maxTotal ? results.slice(0, maxTotal) : results;
}

async function fetchWeiboPosts(config) {
  if (!hasUsableCookie(config.weiboCookie)) {
    return [];
  }

  const endpoint = `https://weibo.com/ajax/statuses/mymblog?uid=${WEIBO_UID}&page=1&feature=0`;
  const headers = buildWeiboHeaders(config.weiboCookie, `https://weibo.com/u/${WEIBO_UID}`);
  const data = await requestJson(endpoint, { headers }, "微博时间线");

  const list = Array.isArray(data?.data?.list)
    ? data.data.list
    : Array.isArray(data?.list)
      ? data.list
      : [];

  return list.slice(0, config.maxPostsPerSource).map((item) => normalizeWeiboPost(item));
}

function dedupePostsById(posts) {
  const deduped = [];
  const seen = new Set();

  for (const post of posts) {
    if (!post?.postId) {
      continue;
    }

    if (seen.has(post.postId)) {
      continue;
    }

    seen.add(post.postId);
    deduped.push(post);
  }

  return deduped;
}

function buildOcrCacheKey(imageUrl, post, provider = "local") {
  const source = String(post?.source || "unknown").trim().toLowerCase();
  const url = String(imageUrl || "").trim();
  const engine = String(provider || "local").trim().toLowerCase();
  return `${source}|${engine}|${url}`;
}

function buildOcrCacheFilePath(cacheKey) {
  const digest = createHash("sha256").update(String(cacheKey || "")).digest("hex");
  return path.join(OCR_CACHE_DIR, `${digest}.txt`);
}

function shouldPreferQwenOcr(config) {
  const provider = normalizeOcrProvider(config?.ocrProvider);
  if (provider === "local") {
    return false;
  }
  return Boolean(resolveQwenApiKey(config));
}

function resolveDashScopeOcrEndpoint() {
  const base = String(process.env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/api/v1")
    .trim()
    .replace(/\/$/, "");
  if (base.endsWith("/services/aigc/multimodal-generation/generation")) {
    return base;
  }
  return `${base}/services/aigc/multimodal-generation/generation`;
}

function stripMarkdownCodeFence(text) {
  const raw = String(text || "").trim();
  const matched = raw.match(/^```[a-zA-Z0-9_-]*\s*([\s\S]*?)\s*```$/);
  return matched ? matched[1].trim() : raw;
}

function decodeHtmlEntities(text) {
  return String(text || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/g, "'");
}

function qwenTableHtmlToText(text) {
  const html = stripMarkdownCodeFence(text);
  if (!html || !/<tr[\s>]/i.test(html)) {
    return html;
  }

  const lines = [];
  for (const rowMatch of html.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const rowHtml = rowMatch[1];
    const cells = [];

    for (const cellMatch of rowHtml.matchAll(/<(td|th)\b[^>]*>([\s\S]*?)<\/\1>/gi)) {
      const cellText = stripHtml(decodeHtmlEntities(cellMatch[2]));
      if (cellText) {
        cells.push(cellText);
      }
    }

    if (cells.length > 0) {
      lines.push(cells.join("\t"));
    }
  }

  return lines.join("\n").trim();
}

function getImageMimeType(filePath) {
  const ext = path.extname(String(filePath || "")).toLowerCase();
  if (ext === ".png") {
    return "image/png";
  }
  if (ext === ".webp") {
    return "image/webp";
  }
  return "image/jpeg";
}

async function filePathToDataUrl(filePath) {
  const bytes = await fs.readFile(filePath);
  return `data:${getImageMimeType(filePath)};base64,${bytes.toString("base64")}`;
}

function getQwenContentBlocks(data) {
  const content = data?.output?.choices?.[0]?.message?.content;
  return Array.isArray(content) ? content : [];
}

function extractTextFromQwenAdvancedRecognition(data) {
  const content = getQwenContentBlocks(data);
  const lines = [];

  for (const item of content) {
    const words = Array.isArray(item?.ocr_result?.words_info) ? item.ocr_result.words_info : [];
    for (const word of words) {
      const line = String(word?.text || "").trim();
      if (line) {
        lines.push(line);
      }
    }
  }

  if (lines.length > 0) {
    return lines.join("\n").trim();
  }

  return content
    .map((item) => stripMarkdownCodeFence(item?.text))
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function extractTextFromQwenTextRecognition(data) {
  return getQwenContentBlocks(data)
    .map((item) => stripMarkdownCodeFence(item?.text))
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function extractTextFromQwenTableParsing(data) {
  return getQwenContentBlocks(data)
    .map((item) => qwenTableHtmlToText(item?.text))
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function normalizeSnapshotSymbol(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) {
    return null;
  }

  if (/^\d{4,6}$/.test(raw)) {
    return raw;
  }

  const normalized = normalizeCodeToken(raw);
  if (normalized && /^\d{4,6}$/.test(normalized)) {
    return normalized;
  }

  return null;
}

function normalizeSnapshotSymbolByName(symbol, name) {
  const normalizedSymbol = normalizeSnapshotSymbol(symbol);
  const normalizedName = normalizeSecurityName(normalizedSymbol || symbol, sanitizeName(name));
  const candidates = SNAPSHOT_SYMBOL_OVERRIDES_BY_NAME[normalizedName];

  if (!Array.isArray(candidates) || candidates.length === 0) {
    return normalizedSymbol;
  }

  if (!normalizedSymbol) {
    return candidates[0];
  }

  const matchedByLength = candidates.find((item) => item.length === normalizedSymbol.length);
  return matchedByLength || normalizedSymbol;
}

function getSnapshotRowQty(row) {
  const candidates = [row?.holdingQty, row?.balanceQty, row?.availableQty, row?.changeQty]
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item) && item > 0);

  if (candidates.length === 0) {
    return 0;
  }

  return Math.max(...candidates);
}

function scoreSnapshotRow(row) {
  let score = 0;
  const numericFields = [
    row?.holdingQty,
    row?.balanceQty,
    row?.availableQty,
    row?.referenceCost,
    row?.latestPrice,
    row?.referenceHoldingCost,
    row?.marketValue,
    row?.floatingPnl,
    row?.pnlPct
  ];

  for (const value of numericFields) {
    if (Number.isFinite(Number(value))) {
      score += 1;
    }
  }

  if (String(row?.name || "").trim()) {
    score += 1;
  }
  if (String(row?.marketName || "").trim()) {
    score += 1;
  }

  return score;
}

function addSnapshotValueRatioPenalty(actual, expected, lower, upper, basePenalty) {
  if (!Number.isFinite(actual) || actual <= 0 || !Number.isFinite(expected) || expected <= 0) {
    return 0;
  }

  const ratio = actual / expected;
  if (ratio >= lower && ratio <= upper) {
    return 0;
  }

  return basePenalty + Math.min(12, Math.abs(Math.log(ratio)) * 2.2);
}

function snapshotRowSanityPenalty(row) {
  const qty = getSnapshotRowQty(row);
  const marketValue = Number(row?.marketValue);
  const latestPrice = Number(row?.latestPrice);
  const referenceCost = Number(row?.referenceCost ?? row?.latestCost);
  const referenceHoldingCost = Number(row?.referenceHoldingCost);
  let penalty = 0;

  if (qty <= 0) {
    penalty += 10;
  }
  if (qty > 50_000_000) {
    penalty += 20;
  }
  if (!Number.isFinite(marketValue) || marketValue < 0) {
    penalty += 20;
  }
  if (Number.isFinite(marketValue) && marketValue > 500_000_000) {
    penalty += 16;
  }
  if (Number.isFinite(latestPrice) && latestPrice > 10_000) {
    penalty += 16;
  }

  if (qty > 0 && Number.isFinite(latestPrice) && latestPrice > 0) {
    penalty += addSnapshotValueRatioPenalty(marketValue, qty * latestPrice, 0.35, 3.2, 12);
  }
  if (qty > 0 && Number.isFinite(referenceCost) && referenceCost > 0) {
    penalty += addSnapshotValueRatioPenalty(marketValue, qty * referenceCost, 0.02, 80, 8);
  }
  if (Number.isFinite(referenceHoldingCost) && referenceHoldingCost > 0) {
    penalty += addSnapshotValueRatioPenalty(marketValue, referenceHoldingCost, 0.02, 80, 8);
  }

  return penalty;
}

function mergeSnapshotRows(primary, secondary) {
  const merged = {
    ...primary
  };

  if (!String(merged.name || "").trim() && String(secondary?.name || "").trim()) {
    merged.name = secondary.name;
  }
  if (!String(merged.marketName || "").trim() && String(secondary?.marketName || "").trim()) {
    merged.marketName = secondary.marketName;
  }

  const numericFields = [
    "changeQty",
    "holdingQty",
    "balanceQty",
    "availableQty",
    "referenceCost",
    "latestCost",
    "latestPrice",
    "referenceHoldingCost",
    "marketValue",
    "floatingPnl",
    "pnlPct"
  ];

  for (const field of numericFields) {
    if (!Number.isFinite(Number(merged[field])) && Number.isFinite(Number(secondary?.[field]))) {
      merged[field] = secondary[field];
    }
  }

  if (getSnapshotRowQty(merged) <= 0 && getSnapshotRowQty(secondary) > 0) {
    merged.changeQty = secondary.changeQty;
    merged.holdingQty = secondary.holdingQty;
    merged.balanceQty = secondary.balanceQty;
    merged.availableQty = secondary.availableQty;
  }

  return merged;
}

function chooseBetterSnapshotRow(left, right) {
  const leftPenalty = snapshotRowSanityPenalty(left);
  const rightPenalty = snapshotRowSanityPenalty(right);

  if (leftPenalty !== rightPenalty) {
    return leftPenalty < rightPenalty ? left : right;
  }

  const leftScore = scoreSnapshotRow(left);
  const rightScore = scoreSnapshotRow(right);
  if (leftScore !== rightScore) {
    return leftScore > rightScore ? left : right;
  }

  const leftQty = getSnapshotRowQty(left);
  const rightQty = getSnapshotRowQty(right);
  if (leftQty !== rightQty) {
    return leftQty > rightQty ? left : right;
  }

  const leftValue = Number(left?.marketValue) || 0;
  const rightValue = Number(right?.marketValue) || 0;
  return leftValue >= rightValue ? left : right;
}

function isSuspiciousSnapshotRow(row) {
  return snapshotRowSanityPenalty(row) >= 16;
}

function shouldRunQwenCompatibleFallback(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return true;
  }

  return rows.some((row) => isSuspiciousSnapshotRow(row));
}

function isClearlyBrokenSnapshotRow(row) {
  const qty = getSnapshotRowQty(row);
  const marketValue = Number(row?.marketValue);
  const latestPrice = Number(row?.latestPrice);
  const referenceCost = Math.abs(Number(row?.referenceCost ?? row?.latestCost));

  return (
    qty > 10_000_000 ||
    marketValue > 100_000_000 ||
    latestPrice > 10_000 ||
    referenceCost > 10_000
  );
}

function filterClearlyBrokenSnapshotRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return [];
  }

  const filtered = rows.filter((row) => !isClearlyBrokenSnapshotRow(row));
  return filtered.length >= Math.max(3, Math.ceil(rows.length * 0.6)) ? filtered : rows;
}

function dedupeSnapshotRows(rows) {
  const deduped = [];
  const seen = new Map();

  for (const row of rows) {
    const symbol = normalizeSnapshotSymbolByName(row?.symbol, row?.name);
    if (!row || !symbol) {
      continue;
    }

    const normalizedRow = {
      ...row,
      symbol,
      name: normalizeSnapshotNameBySymbol(symbol, row.name)
    };

    const key = symbol;

    const existingIndex = seen.get(key);
    if (existingIndex === undefined) {
      seen.set(key, deduped.length);
      deduped.push(normalizedRow);
      continue;
    }

    const existing = deduped[existingIndex];
    const preferred = chooseBetterSnapshotRow(existing, normalizedRow);
    const secondary = preferred === existing ? normalizedRow : existing;
    deduped[existingIndex] = mergeSnapshotRows(preferred, secondary);
  }

  return filterClearlyBrokenSnapshotRows(deduped);
}

function normalizeQwenSnapshotRow(input) {
  const nameText = input?.name || input?.stockName || input?.securityName || "";
  const symbol = normalizeSnapshotSymbolByName(
    input?.symbol || input?.code || input?.stockCode || input?.securityCode || input?.ticker,
    nameText
  );
  const name = normalizeSnapshotNameBySymbol(symbol, nameText);

  const holdingQty = parseNumeric(input?.holdingQty ?? input?.quantity ?? input?.qty ?? input?.availableQty);
  const referenceCost = parseNumeric(input?.referenceCost ?? input?.cost ?? input?.latestCost);
  const referenceHoldingCost = parseNumeric(
    input?.referenceHoldingCost ?? input?.holdingCost ?? input?.costAmount ?? input?.totalCost
  );
  const marketValue = parseNumeric(input?.marketValue ?? input?.value ?? input?.latestMarketValue);
  const latestPrice = parseNumeric(input?.latestPrice ?? input?.price);
  const pnlPct = parseNumeric(input?.pnlPct ?? input?.profitPct ?? input?.profitRatio);
  const marketName = normalizeSnapshotMarketName(symbol, input?.marketName || input?.market || "");

  if (!symbol || !name || !Number.isFinite(holdingQty) || !Number.isFinite(referenceCost) || !Number.isFinite(marketValue)) {
    return null;
  }

  const inferredHoldingCost =
    normalizeReferenceHoldingCost(holdingQty, referenceCost, referenceHoldingCost);
  const inferredLatestPrice =
    Number.isFinite(latestPrice) ? latestPrice : holdingQty > 0 ? Number(marketValue) / Number(holdingQty) : null;
  const floatingPnl =
    Number.isFinite(inferredHoldingCost) && inferredHoldingCost > 0 && Number.isFinite(marketValue)
      ? Number(marketValue) - inferredHoldingCost
      : null;

  const normalizedRow = {
    symbol,
    name,
    actionLabel: "持仓快照",
    action: "HOLD",
    changeQty: Number(holdingQty),
    latestCost: Number(referenceCost),
    holdingQty: Number(holdingQty),
    balanceQty: Number(holdingQty),
    availableQty: Number(holdingQty),
    referenceCost: Number(referenceCost),
    latestPrice: Number.isFinite(inferredLatestPrice) ? Number(inferredLatestPrice) : null,
    referenceHoldingCost: Number.isFinite(inferredHoldingCost) ? Number(inferredHoldingCost) : null,
    marketValue: Number(marketValue),
    floatingPnl: Number.isFinite(floatingPnl) ? Number(floatingPnl) : null,
    pnlPct: Number.isFinite(pnlPct) ? Number(pnlPct) : null,
    marketName
  };

  return /^\d{6}$/.test(symbol) ? normalizeAshareSnapshotRow(normalizedRow) : normalizeHkSnapshotRow(normalizedRow);
}

function normalizeQwenHeaderText(value) {
  return String(value || "")
    .replace(/\s+/g, "")
    .replace(/[（(][^）)]*[）)]/g, "")
    .replace(/人民币/g, "")
    .replace(/[%％]/g, "")
    .replace(/[：:]/g, "")
    .trim();
}

function scoreQwenHeaderMatch(text, aliases = []) {
  const normalizedText = normalizeQwenHeaderText(text);
  if (!normalizedText) {
    return 0;
  }

  let best = 0;
  for (const alias of aliases) {
    const normalizedAlias = normalizeQwenHeaderText(alias);
    if (!normalizedAlias) {
      continue;
    }
    if (normalizedText === normalizedAlias) {
      best = Math.max(best, normalizedAlias.length + 20);
      continue;
    }
    if (normalizedText.includes(normalizedAlias) || normalizedAlias.includes(normalizedText)) {
      best = Math.max(best, normalizedAlias.length);
    }
  }
  return best;
}

function normalizeQwenOcrBlock(input) {
  const text = String(input?.text || "").trim();
  if (!text) {
    return null;
  }

  let x = Number(input?.x);
  let y = Number(input?.y);
  let width = Number(input?.width);
  let height = Number(input?.height);

  if ((!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(width) || !Number.isFinite(height)) && Array.isArray(input?.location)) {
    const values = input.location.map((item) => Number(item)).filter((item) => Number.isFinite(item));
    const xs = values.filter((_, index) => index % 2 === 0);
    const ys = values.filter((_, index) => index % 2 === 1);
    if (xs.length > 0 && ys.length > 0) {
      const minX = Math.min(...xs);
      const maxX = Math.max(...xs);
      const minY = Math.min(...ys);
      const maxY = Math.max(...ys);
      x = minX;
      y = minY;
      width = maxX - minX;
      height = maxY - minY;
    }
  }

  if ((!Number.isFinite(x) || !Number.isFinite(y) || !Number.isFinite(width) || !Number.isFinite(height)) && Array.isArray(input?.rotate_rect)) {
    const values = input.rotate_rect.map((item) => Number(item));
    if (values.length >= 4 && values.slice(0, 4).every((item) => Number.isFinite(item))) {
      [x, y, width, height] = values;
    }
  }

  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    return null;
  }

  const normalizedWidth = Number.isFinite(width) && width > 0 ? width : 1;
  const normalizedHeight = Number.isFinite(height) && height > 0 ? height : 1;

  return {
    text,
    x,
    y,
    width: normalizedWidth,
    height: normalizedHeight,
    right: x + normalizedWidth,
    bottom: y + normalizedHeight,
    cx: x + normalizedWidth / 2,
    cy: y + normalizedHeight / 2
  };
}

function parseQwenOcrBlocksFromText(text) {
  const cleaned = stripMarkdownCodeFence(text);
  const parsed = safeJsonParse(cleaned);
  const blocks = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.blocks) ? parsed.blocks : [];
  return blocks.map((item) => normalizeQwenOcrBlock(item)).filter(Boolean);
}

function extractQwenOcrBlocks(data) {
  const content = getQwenContentBlocks(data);
  const blocks = [];

  for (const item of content) {
    const words = Array.isArray(item?.ocr_result?.words_info) ? item.ocr_result.words_info : [];
    const wordBlocks = words.map((word) => normalizeQwenOcrBlock(word)).filter(Boolean);
    if (wordBlocks.length > 0) {
      blocks.push(...wordBlocks);
      continue;
    }

    const processedBlocks = parseQwenOcrBlocksFromText(item?.ocr_result?.processed_text);
    if (processedBlocks.length > 0) {
      blocks.push(...processedBlocks);
    }
  }

  const deduped = [];
  const seen = new Set();
  for (const block of blocks) {
    const key = `${block.text}|${Math.round(block.x)}|${Math.round(block.y)}|${Math.round(block.width)}|${Math.round(block.height)}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(block);
  }

  return deduped.sort((a, b) => a.cy - b.cy || a.x - b.x);
}

function qwenBlocksToPlainText(blocks) {
  if (!Array.isArray(blocks) || blocks.length === 0) {
    return "";
  }

  const sorted = [...blocks].sort((a, b) => a.cy - b.cy || a.x - b.x);
  const lineTolerance = Math.max(18, Math.min(42, (computeMedian(sorted.map((item) => item.height)) || 24) * 0.8));
  const lines = [];

  for (const block of sorted) {
    const current = lines[lines.length - 1];
    if (!current || Math.abs(block.cy - current.cy) > lineTolerance) {
      lines.push({ cy: block.cy, blocks: [block] });
      continue;
    }
    current.blocks.push(block);
  }

  return lines
    .map((line) => line.blocks.sort((a, b) => a.x - b.x).map((item) => item.text).join("\t"))
    .join("\n")
    .trim();
}

function findQwenSchemaHeaderBlocks(blocks, schema) {
  const headerBlocks = {};

  for (const column of schema.columns) {
    let picked = null;
    let pickedScore = 0;

    for (const block of blocks) {
      const score = scoreQwenHeaderMatch(block.text, column.aliases);
      if (score <= 0) {
        continue;
      }

      if (!picked || score > pickedScore || (score === pickedScore && block.y < picked.y)) {
        picked = block;
        pickedScore = score;
      }
    }

    if (picked) {
      headerBlocks[column.key] = picked;
    }
  }

  return headerBlocks;
}

function resolveQwenTableSchema(blocks) {
  let best = null;

  for (const schema of QWEN_TABLE_SCHEMAS) {
    const headerBlocks = findQwenSchemaHeaderBlocks(blocks, schema);
    const score = Object.keys(headerBlocks).length;
    if (!best || score > best.score) {
      best = { schema, headerBlocks, score };
    }
  }

  if (!best || best.score < 5 || !best.headerBlocks.symbol || !best.headerBlocks.name) {
    return null;
  }

  return best;
}

function buildQwenColumnLayout(schema, headerBlocks) {
  const ordered = schema.columns
    .map((column) => {
      const header = headerBlocks[column.key];
      if (!header) {
        return null;
      }
      return {
        ...column,
        header,
        x: header.cx,
        cx: header.cx
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.x - b.x);

  return ordered.map((column, index) => {
    const previous = ordered[index - 1];
    const next = ordered[index + 1];
    return {
      ...column,
      left: previous ? (previous.x + column.x) / 2 : -Infinity,
      right: next ? (column.x + next.x) / 2 : Infinity
    };
  });
}

function computeQwenHeaderBoundary(columns) {
  const headerYs = columns.map((column) => column.header.y);
  const headerHeights = columns.map((column) => Math.min(column.header.height, 80));
  return (computeMedian(headerYs) || 0) + (computeMedian(headerHeights) || 0) + 6;
}

function findQwenRowAnchors(blocks, schema, symbolColumn, headerBoundary) {
  if (!symbolColumn) {
    return [];
  }

  const candidates = blocks
    .filter((block) => block.bottom > headerBoundary)
    .filter((block) => block.x >= symbolColumn.left && block.x < symbolColumn.right)
    .filter((block) => schema.codePattern.test(String(block.text || "").trim()))
    .sort((a, b) => a.cy - b.cy || a.x - b.x);

  if (candidates.length === 0) {
    return [];
  }

  const gaps = [];
  for (let i = 1; i < candidates.length; i += 1) {
    const gap = candidates[i].cy - candidates[i - 1].cy;
    if (gap > 4) {
      gaps.push(gap);
    }
  }

  const rowTolerance = Math.max(18, Math.min(42, (computeMedian(gaps) || computeMedian(candidates.map((item) => item.height)) || 30) * 0.42));
  const anchors = [];

  for (const candidate of candidates) {
    const previous = anchors[anchors.length - 1];
    if (previous && Math.abs(candidate.cy - previous.cy) <= rowTolerance) {
      if (candidate.x < previous.x) {
        anchors[anchors.length - 1] = candidate;
      }
      continue;
    }
    anchors.push(candidate);
  }

  return anchors;
}

function findClosestQwenColumn(block, columns) {
  let picked = columns[0];
  let minDistance = Math.abs(block.cx - columns[0].x);

  for (const column of columns) {
    if (block.cx >= column.left && block.cx < column.right) {
      return column;
    }
    const distance = Math.abs(block.cx - column.x);
    if (distance < minDistance) {
      minDistance = distance;
      picked = column;
    }
  }

  return picked;
}

function buildQwenRowCells(blocks, columns, anchorIndex, anchors, headerBoundary) {
  const currentAnchor = anchors[anchorIndex];
  const previousAnchor = anchors[anchorIndex - 1];
  const nextAnchor = anchors[anchorIndex + 1];
  const upperBound = previousAnchor ? (previousAnchor.cy + currentAnchor.cy) / 2 : headerBoundary;
  const lowerBound = nextAnchor ? (currentAnchor.cy + nextAnchor.cy) / 2 : Infinity;
  const cells = Object.fromEntries(columns.map((column) => [column.key, []]));

  for (const block of blocks) {
    if (block.bottom <= headerBoundary || block.cy <= upperBound || block.cy > lowerBound) {
      continue;
    }

    const column = findClosestQwenColumn(block, columns);
    if (!column) {
      continue;
    }
    cells[column.key].push(block);
  }

  return Object.fromEntries(
    Object.entries(cells).map(([key, value]) => [
      key,
      value
        .sort((a, b) => a.x - b.x)
        .map((item) => item.text)
        .join("")
        .trim()
    ])
  );
}

function buildAShareSnapshotRowFromQwenCells(cells) {
  const symbol = normalizeSnapshotSymbolByName(cells.symbol, cells.name);
  const name = normalizeSnapshotNameBySymbol(symbol, cells.name);
  const balanceQty = parseOcrNumericCell(cells.balanceQty);
  const availableQty = parseOcrNumericCell(cells.availableQty);
  const holdingQty = Number.isFinite(availableQty) ? availableQty : balanceQty;
  let referenceCost = parseOcrNumericCell(cells.referenceCost);
  let latestPrice = parseOcrNumericCell(cells.latestPrice);
  const marketValue = parseOcrNumericCell(cells.marketValue);
  const floatingPnl = parseOcrNumericCell(cells.floatingPnl);
  const pnlPct = parseOcrNumericCell(cells.pnlPct);
  let referenceHoldingCost =
    Number.isFinite(marketValue) && Number.isFinite(floatingPnl) ? marketValue - floatingPnl : null;

  if (!Number.isFinite(referenceHoldingCost) && Number.isFinite(referenceCost) && Number.isFinite(holdingQty)) {
    referenceHoldingCost = referenceCost * holdingQty;
  }

  if ((!Number.isFinite(referenceCost) || isTruncatedCellText(cells.referenceCost)) && Number.isFinite(referenceHoldingCost) && holdingQty > 0) {
    referenceCost = referenceHoldingCost / holdingQty;
  }

  if ((!Number.isFinite(latestPrice) || isTruncatedCellText(cells.latestPrice)) && Number.isFinite(marketValue) && holdingQty > 0) {
    latestPrice = marketValue / holdingQty;
  }

  if (Number.isFinite(latestPrice) && Number.isFinite(marketValue) && holdingQty > 0) {
    const derivedPrice = marketValue / holdingQty;
    const drift = Math.abs(derivedPrice - latestPrice) / Math.max(Math.abs(derivedPrice), 1);
    if (drift > 0.25) {
      latestPrice = derivedPrice;
    }
  }

  if (
    !symbol ||
    !name ||
    !Number.isFinite(holdingQty) ||
    !Number.isFinite(referenceCost) ||
    !Number.isFinite(marketValue)
  ) {
    return null;
  }

  return normalizeAshareSnapshotRow({
    symbol,
    name,
    actionLabel: "持仓快照",
    action: "HOLD",
    changeQty: Number(holdingQty),
    latestCost: Number(referenceCost),
    holdingQty: Number(holdingQty),
    balanceQty: Number.isFinite(balanceQty) ? Number(balanceQty) : Number(holdingQty),
    availableQty: Number.isFinite(availableQty) ? Number(availableQty) : Number(holdingQty),
    referenceCost: Number(referenceCost),
    latestPrice: Number.isFinite(latestPrice) ? Number(latestPrice) : null,
    referenceHoldingCost: Number.isFinite(referenceHoldingCost) ? Number(referenceHoldingCost) : Number(referenceCost) * Number(holdingQty),
    marketValue: Number(marketValue),
    floatingPnl: Number.isFinite(floatingPnl) ? Number(floatingPnl) : null,
    pnlPct: Number.isFinite(pnlPct) ? Number(pnlPct) : null,
    marketName: sanitizeName(cells.marketName) || inferCnMarketName(symbol)
  });
}

function buildHkSnapshotRowFromQwenCells(cells) {
  const symbol = normalizeSnapshotSymbolByName(cells.symbol, cells.name);
  const name = normalizeSnapshotNameBySymbol(symbol, cells.name);
  const holdingQtyRaw = parseOcrNumericCell(cells.holdingQty);
  let referenceCost = parseOcrNumericCell(cells.referenceCost);
  let referenceHoldingCost = parseOcrNumericCell(cells.referenceHoldingCost);
  const marketValue = parseOcrNumericCell(cells.marketValue);
  const pnlPct = parseOcrNumericCell(cells.pnlPct);
  const holdingQty = reconcileHoldingQty(holdingQtyRaw, referenceCost, referenceHoldingCost, symbol);
  const latestPrice = Number.isFinite(marketValue) && Number.isFinite(holdingQty) && holdingQty > 0 ? marketValue / holdingQty : null;

  referenceHoldingCost = normalizeReferenceHoldingCost(holdingQty, referenceCost, referenceHoldingCost);

  if ((!Number.isFinite(referenceCost) || isTruncatedCellText(cells.referenceCost)) && Number.isFinite(referenceHoldingCost) && holdingQty > 0) {
    referenceCost = referenceHoldingCost / holdingQty;
  }

  if (
    !symbol ||
    !name ||
    !Number.isFinite(holdingQty) ||
    !Number.isFinite(referenceCost) ||
    !Number.isFinite(marketValue)
  ) {
    return null;
  }

  const floatingPnl =
    Number.isFinite(referenceHoldingCost) && referenceHoldingCost > 0 && Number.isFinite(marketValue)
      ? marketValue - referenceHoldingCost
      : null;

  return normalizeHkSnapshotRow({
    symbol,
    name,
    actionLabel: "持仓快照",
    action: "HOLD",
    changeQty: Number(holdingQty),
    latestCost: Number(referenceCost),
    holdingQty: Number(holdingQty),
    balanceQty: Number(holdingQty),
    availableQty: Number(holdingQty),
    referenceCost: Number(referenceCost),
    latestPrice: Number.isFinite(latestPrice) ? Number(latestPrice) : null,
    referenceHoldingCost: Number.isFinite(referenceHoldingCost) ? Number(referenceHoldingCost) : Number(referenceCost) * Number(holdingQty),
    marketValue: Number(marketValue),
    floatingPnl: Number.isFinite(floatingPnl) ? Number(floatingPnl) : null,
    pnlPct: Number.isFinite(pnlPct) ? Number(pnlPct) : null,
    marketName: "港股通"
  });
}

function parseSnapshotRowsFromQwenBlocks(blocks) {
  const resolved = resolveQwenTableSchema(blocks);
  if (!resolved) {
    return [];
  }

  const columns = buildQwenColumnLayout(resolved.schema, resolved.headerBlocks);
  const symbolColumn = columns.find((column) => column.key === "symbol");
  if (!symbolColumn) {
    return [];
  }

  const headerBoundary = computeQwenHeaderBoundary(columns);
  const rowAnchors = findQwenRowAnchors(blocks, resolved.schema, symbolColumn, headerBoundary);
  const rows = [];

  for (let i = 0; i < rowAnchors.length; i += 1) {
    const cells = buildQwenRowCells(blocks, columns, i, rowAnchors, headerBoundary);
    const row =
      resolved.schema.key === "hk"
        ? buildHkSnapshotRowFromQwenCells(cells)
        : buildAShareSnapshotRowFromQwenCells(cells);

    if (row) {
      rows.push(row);
    }
  }

  return dedupeSnapshotRows(rows);
}

async function prepareImageForQwenOcr(localPath) {
  const scaledPath = path.join(
    os.tmpdir(),
    `stock-lu-qwen-upscaled-${Date.now()}-${Math.random().toString(16).slice(2)}.png`
  );

  try {
    await execFileAsync("sips", ["-Z", "2400", localPath, "--out", scaledPath]);
    return {
      imagePath: scaledPath,
      cleanupPath: scaledPath
    };
  } catch {
    return {
      imagePath: localPath,
      cleanupPath: null
    };
  }
}

async function extractRowsWithQwenNativeOcr(imageUrl, post, config) {
  const cacheKey = buildOcrCacheKey(imageUrl, post, "qwen-native");
  const cached = await getCachedOcrText(cacheKey);
  if (cached) {
    const cachedBlocks = parseQwenOcrBlocksFromText(cached);
    return {
      rows: parseSnapshotRowsFromQwenBlocks(cachedBlocks),
      rawText: qwenBlocksToPlainText(cachedBlocks)
    };
  }

  const localPath = await downloadImageToTempFile(imageUrl, post, config);
  let prepared = { imagePath: localPath, cleanupPath: null };

  try {
    prepared = await prepareImageForQwenOcr(localPath);
    const advancedData = await requestQwenOcrTask(prepared.imagePath, "advanced_recognition", config);
    const blocks = extractQwenOcrBlocks(advancedData);
    if (blocks.length === 0) {
      return {
        rows: [],
        rawText: ""
      };
    }

    const cacheValue = JSON.stringify(
      blocks.map((block) => ({
        text: block.text,
        x: block.x,
        y: block.y,
        width: block.width,
        height: block.height
      }))
    );
    setCachedOcrText(cacheKey, cacheValue);

    return {
      rows: parseSnapshotRowsFromQwenBlocks(blocks),
      rawText: qwenBlocksToPlainText(blocks)
    };
  } finally {
    if (prepared.cleanupPath && prepared.cleanupPath !== localPath) {
      await fs.unlink(prepared.cleanupPath).catch(() => {});
    }
    await fs.unlink(localPath).catch(() => {});
  }
}

function resolveQwenCompatibleBaseUrl() {
  return String(process.env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/compatible-mode/v1")
    .trim()
    .replace(/\/$/, "");
}

async function requestQwenCompatibleCompletion(localPath, prompt, config) {
  const apiKey = resolveQwenApiKey(config);
  if (!apiKey) {
    throw new Error("Qwen OCR API Key 未配置");
  }

  const baseUrl = resolveQwenCompatibleBaseUrl();
  const imageUrl = await filePathToDataUrl(localPath);
  const response = await fetchWithTimeout(
    `${baseUrl}/chat/completions`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: QWEN_VL_OCR_MODEL,
        temperature: 0,
        messages: [
          {
            role: "system",
            content:
              "你是一个股票持仓截图结构化提取助手。请严格输出用户要求的 JSON，不要输出任何额外解释、代码块或注释。"
          },
          {
            role: "user",
            content: [
              {
                type: "text",
                text: prompt
              },
              {
                type: "image_url",
                image_url: {
                  url: imageUrl
                }
              }
            ]
          }
        ]
      })
    },
    QWEN_NATIVE_OCR_TIMEOUT_MS
  );

  const rawText = await response.text();
  const data = safeJsonParse(rawText);
  if (!response.ok) {
    const detail =
      data?.message || data?.code || data?.error || data?.error_description || shortText(rawText, 240) || "未知错误";
    throw new Error(`Qwen OCR 请求失败 (${response.status}): ${detail}`);
  }

  const content = data?.choices?.[0]?.message?.content;
  if (typeof content !== "string" || !content.trim()) {
    throw new Error("Qwen OCR 未返回可用内容");
  }

  return content.trim();
}

function parseQwenSnapshotRowsFromContent(content) {
  const cleaned = stripMarkdownCodeFence(content);
  const parsed = safeJsonParse(cleaned);
  const rows = Array.isArray(parsed)
    ? parsed
    : Array.isArray(parsed?.rows)
      ? parsed.rows
      : Array.isArray(parsed?.data)
        ? parsed.data
        : [];

  return dedupeSnapshotRows(rows.map((item) => normalizeQwenSnapshotRow(item)).filter(Boolean));
}

async function extractRowsWithQwenCompatibleOcr(imageUrl, post, config) {
  const cacheKey = buildOcrCacheKey(imageUrl, post, "qwen-json");
  const cached = await getCachedOcrText(cacheKey);
  if (cached) {
    return {
      rows: parseQwenSnapshotRowsFromContent(cached),
      rawText: cached
    };
  }

  const localPath = await downloadImageToTempFile(imageUrl, post, config);
  const prompt = [
    "请识别这张股票持仓截图，只返回 JSON 数组。",
    "每个数组元素包含以下字段：symbol,name,holdingQty,referenceCost,referenceHoldingCost,marketValue,pnlPct,latestPrice,marketName。",
    "要求：",
    "1. 证券代码保留前导0。",
    "2. 只提取真正的持仓行，忽略资金余额、提示、表头、页脚、水印、按钮、说明文字。",
    "3. 数字字段只返回数字或 null，不要带货币符号、百分号、逗号。",
    "4. 如果截图没有 latestPrice，可填 null；如果没有 marketName，填空字符串。",
    "5. 不要返回 markdown 代码块，不要返回任何解释。"
  ].join("\n");

  try {
    const rawText = await requestQwenCompatibleCompletion(localPath, prompt, config);
    setCachedOcrText(cacheKey, rawText);
    return {
      rows: parseQwenSnapshotRowsFromContent(rawText),
      rawText
    };
  } finally {
    await fs.unlink(localPath).catch(() => {});
  }
}

function pruneOcrTextCache(now = Date.now()) {
  for (const [key, entry] of ocrTextCache.entries()) {
    const createdAt = Number(entry?.createdAt) || 0;
    if (!createdAt || now - createdAt > OCR_CACHE_TTL_MS) {
      ocrTextCache.delete(key);
    }
  }

  if (ocrTextCache.size <= OCR_CACHE_MAX_ITEMS) {
    return;
  }

  const sorted = [...ocrTextCache.entries()].sort((a, b) => {
    const aUsed = Number(a[1]?.usedAt) || 0;
    const bUsed = Number(b[1]?.usedAt) || 0;
    return aUsed - bUsed;
  });

  const removeCount = ocrTextCache.size - OCR_CACHE_MAX_ITEMS;
  for (let i = 0; i < removeCount; i += 1) {
    const key = sorted[i]?.[0];
    if (key) {
      ocrTextCache.delete(key);
    }
  }
}

async function ensureOcrDiskCacheDir() {
  if (!OCR_DISK_CACHE_ENABLED) {
    return;
  }

  if (!ocrDiskCacheInitPromise) {
    ocrDiskCacheInitPromise = fs.mkdir(OCR_CACHE_DIR, { recursive: true }).finally(() => {
      ocrDiskCacheInitPromise = null;
    });
  }

  await ocrDiskCacheInitPromise;
}

async function pruneOcrDiskCache(now = Date.now()) {
  if (!OCR_DISK_CACHE_ENABLED) {
    return;
  }

  await ensureOcrDiskCacheDir();
  const entries = await fs.readdir(OCR_CACHE_DIR, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    const filePath = path.join(OCR_CACHE_DIR, entry.name);
    let stats;
    try {
      stats = await fs.stat(filePath);
    } catch {
      continue;
    }

    if (!Number.isFinite(stats.mtimeMs) || now - stats.mtimeMs > OCR_CACHE_TTL_MS) {
      await fs.unlink(filePath).catch(() => {});
      continue;
    }

    files.push({
      path: filePath,
      mtimeMs: stats.mtimeMs
    });
  }

  if (files.length <= OCR_CACHE_MAX_ITEMS) {
    return;
  }

  const removeCount = files.length - OCR_CACHE_MAX_ITEMS;
  const sorted = files.sort((a, b) => a.mtimeMs - b.mtimeMs);
  await Promise.all(sorted.slice(0, removeCount).map((item) => fs.unlink(item.path).catch(() => {})));
}

function scheduleOcrDiskCachePrune(now = Date.now()) {
  if (!OCR_DISK_CACHE_ENABLED) {
    return;
  }

  if (ocrDiskCachePrunePromise || now - lastOcrDiskCachePruneAt < OCR_DISK_CACHE_PRUNE_INTERVAL_MS) {
    return;
  }

  ocrDiskCachePrunePromise = pruneOcrDiskCache(now)
    .catch((error) => {
      console.error("Failed to prune OCR disk cache:", error.message);
    })
    .finally(() => {
      lastOcrDiskCachePruneAt = Date.now();
      ocrDiskCachePrunePromise = null;
    });
}

async function getCachedOcrText(cacheKey) {
  if (!cacheKey) {
    return null;
  }

  const entry = ocrTextCache.get(cacheKey);
  if (!entry) {
    return null;
  }

  const now = Date.now();
  const createdAt = Number(entry.createdAt) || 0;
  if (!createdAt || now - createdAt > OCR_CACHE_TTL_MS) {
    ocrTextCache.delete(cacheKey);
  } else {
    entry.usedAt = now;
    return String(entry.text || "");
  }

  if (!OCR_DISK_CACHE_ENABLED) {
    return null;
  }

  try {
    await ensureOcrDiskCacheDir();
    const filePath = buildOcrCacheFilePath(cacheKey);
    const stats = await fs.stat(filePath);
    if (!Number.isFinite(stats.mtimeMs) || now - stats.mtimeMs > OCR_CACHE_TTL_MS) {
      await fs.unlink(filePath).catch(() => {});
      return null;
    }

    const text = await fs.readFile(filePath, "utf8");
    if (!String(text || "").trim()) {
      return null;
    }

    ocrTextCache.set(cacheKey, {
      text,
      createdAt: stats.mtimeMs,
      usedAt: now
    });
    pruneOcrTextCache(now);
    return text;
  } catch {
    return null;
  }
}

function setCachedOcrText(cacheKey, text) {
  const normalizedText = String(text || "");
  if (!cacheKey || !normalizedText.trim()) {
    return;
  }

  const now = Date.now();
  ocrTextCache.set(cacheKey, {
    text: normalizedText,
    createdAt: now,
    usedAt: now
  });
  pruneOcrTextCache(now);
  scheduleOcrDiskCachePrune(now);

  if (!OCR_DISK_CACHE_ENABLED) {
    return;
  }

  const filePath = buildOcrCacheFilePath(cacheKey);
  const tmpPath = `${filePath}.${process.pid}.${Math.random().toString(16).slice(2)}.tmp`;

  ocrDiskCacheWriteQueue = ocrDiskCacheWriteQueue
    .catch(() => {})
    .then(async () => {
      await ensureOcrDiskCacheDir();
      await fs.writeFile(tmpPath, normalizedText, "utf8");
      await fs.rename(tmpPath, filePath);
    })
    .catch((error) => {
      console.error("Failed to write OCR disk cache:", error.message);
      return fs.unlink(tmpPath).catch(() => {});
    });
}

async function mapWithConcurrency(list, concurrency, worker) {
  const items = Array.isArray(list) ? list : [];
  const maxConcurrency = clampNumber(concurrency, 1, 1, 12);
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runWorker() {
    while (true) {
      const index = nextIndex;
      if (index >= items.length) {
        return;
      }
      nextIndex += 1;
      results[index] = await worker(items[index], index);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(maxConcurrency, items.length) }, () => runWorker())
  );

  return results;
}

async function requestQwenOcrTask(localPath, task, config) {
  const apiKey = resolveQwenApiKey(config);
  if (!apiKey) {
    throw new Error("Qwen OCR API Key 未配置");
  }

  const endpoint = resolveDashScopeOcrEndpoint();
  const image = await filePathToDataUrl(localPath);
  const response = await fetchWithTimeout(
    endpoint,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: QWEN_VL_OCR_MODEL,
        input: {
          messages: [
            {
              role: "user",
              content: [
                {
                  type: "image",
                  image
                }
              ]
            }
          ]
        },
        parameters: {
          result_format: "message",
          ocr_options: {
            task
          }
        }
      })
    },
    QWEN_NATIVE_OCR_TIMEOUT_MS
  );

  const rawText = await response.text();
  const data = safeJsonParse(rawText);
  if (!response.ok) {
    const detail =
      data?.message || data?.code || data?.error || data?.error_description || shortText(rawText, 240) || "未知错误";
    throw new Error(`Qwen OCR 请求失败 (${response.status}): ${detail}`);
  }

  if (!data) {
    throw new Error("Qwen OCR 返回非 JSON");
  }

  return data;
}

async function extractTextWithQwenOcr(localPath, _post, config) {
  const advancedData = await requestQwenOcrTask(localPath, "advanced_recognition", config);
  const advancedText = extractTextFromQwenAdvancedRecognition(advancedData);
  if (extractRowsFromText(advancedText).length > 0) {
    return advancedText;
  }

  const tableData = await requestQwenOcrTask(localPath, "table_parsing", config);
  const tableText = extractTextFromQwenTableParsing(tableData);
  const mergedTableText = [advancedText, tableText].filter(Boolean).join("\n").trim();
  if (extractRowsFromText(mergedTableText).length > 0) {
    return mergedTableText;
  }

  const textData = await requestQwenOcrTask(localPath, "text_recognition", config);
  const plainText = extractTextFromQwenTextRecognition(textData);
  return [advancedText, tableText, plainText].filter(Boolean).join("\n").trim();
}

async function getOcrWorker() {
  if (!ocrWorkerPromise) {
    ocrWorkerPromise = createWorker("chi_sim+eng", 1, {
      logger: () => {},
      errorHandler: () => {}
    })
      .then(async (worker) => {
        await worker.setParameters({
          tessedit_pageseg_mode: "6",
          preserve_interword_spaces: "1"
        });
        return worker;
      })
      .catch((error) => {
        ocrWorkerPromise = null;
        throw error;
      });
  }

  return ocrWorkerPromise;
}

function getImageHeadersForPost(post, config) {
  const headers = {
    "User-Agent": "Mozilla/5.0",
    Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
  };

  if (post.source === "xueqiu") {
    headers.Referer = post.link || `https://xueqiu.com/u/${XUEQIU_UID}`;
    if (hasUsableCookie(config.xueqiuCookie)) {
      headers.Cookie = config.xueqiuCookie;
    }
  } else if (post.source === "weibo") {
    headers.Referer = post.link || `https://weibo.com/u/${WEIBO_UID}`;
    if (hasUsableCookie(config.weiboCookie)) {
      headers.Cookie = config.weiboCookie;
    }
  }

  return headers;
}

function getImageFileExt(contentType, imageUrl) {
  const type = String(contentType || "").toLowerCase();
  if (type.includes("png")) {
    return "png";
  }
  if (type.includes("webp")) {
    return "webp";
  }
  if (type.includes("jpeg") || type.includes("jpg")) {
    return "jpg";
  }

  const lowerUrl = String(imageUrl || "").toLowerCase();
  if (lowerUrl.includes(".png")) {
    return "png";
  }
  if (lowerUrl.includes(".webp")) {
    return "webp";
  }
  return "jpg";
}

async function downloadImageToTempFile(imageUrl, post, config) {
  const headers = getImageHeadersForPost(post, config);
  const response = await fetchWithTimeout(
    imageUrl,
    {
      headers
    },
    30000
  );

  if (!response.ok) {
    throw new Error(`图片下载失败 (${response.status})`);
  }

  const arrayBuffer = await response.arrayBuffer();
  const bytes = Buffer.from(arrayBuffer);
  if (bytes.length < 200) {
    throw new Error("图片内容异常（字节过小）");
  }

  const contentType = response.headers.get("content-type");
  const ext = getImageFileExt(contentType, imageUrl);
  const filePath = path.join(
    os.tmpdir(),
    `stock-lu-ocr-${Date.now()}-${Math.random().toString(16).slice(2)}.${ext}`
  );

  await fs.writeFile(filePath, bytes);
  return filePath;
}

async function extractTextWithLocalOcr(localPath, scaledPath) {
  const run = localOcrQueue.then(async () => {
    const worker = await getOcrWorker();
    const texts = [];

    const baseResult = await worker.recognize(localPath);
    const baseText = String(baseResult?.data?.text || "");
    texts.push(baseText);
    const baseRows = extractRowsFromText(baseText);

    if (baseRows.length === 0) {
      try {
        await execFileAsync("sips", ["-Z", "1600", localPath, "--out", scaledPath]);
        const scaledResult = await worker.recognize(scaledPath);
        texts.push(String(scaledResult?.data?.text || ""));
      } catch {
        // Ignore platform/image preprocessing failures and keep base OCR output.
      }
    }

    return texts.filter(Boolean).join("\n");
  });

  localOcrQueue = run.catch(() => {});
  return run;
}

async function extractTextWithOcr(imageUrl, post, config) {
  const preferQwen = shouldPreferQwenOcr(config);
  const qwenCacheKey = buildOcrCacheKey(imageUrl, post, "qwen");
  const localCacheKey = buildOcrCacheKey(imageUrl, post, "local");

  if (preferQwen) {
    const cachedQwen = await getCachedOcrText(qwenCacheKey);
    if (cachedQwen) {
      return cachedQwen;
    }
  } else {
    const cachedLocal = await getCachedOcrText(localCacheKey);
    if (cachedLocal) {
      return cachedLocal;
    }
  }

  const localPath = await downloadImageToTempFile(imageUrl, post, config);
  const scaledPath = path.join(
    os.tmpdir(),
    `stock-lu-ocr-upscaled-${Date.now()}-${Math.random().toString(16).slice(2)}.png`
  );

  try {
    if (preferQwen) {
      try {
        const qwenText = await extractTextWithQwenOcr(localPath, post, config);
        if (qwenText.trim()) {
          setCachedOcrText(qwenCacheKey, qwenText);
          return qwenText;
        }
      } catch {
        // Ignore remote OCR failure and fall back to local OCR.
      }
    }

    const cachedLocal = await getCachedOcrText(localCacheKey);
    if (cachedLocal) {
      return cachedLocal;
    }

    const localText = await extractTextWithLocalOcr(localPath, scaledPath);
    setCachedOcrText(localCacheKey, localText);
    return localText;
  } finally {
    await fs.unlink(scaledPath).catch(() => {});
    await fs.unlink(localPath).catch(() => {});
  }
}

async function parseSnapshotFromPost(post, config) {
  const textRows = extractRowsFromText(post.text);
  let parsedRows = textRows;
  let ocrText = "";
  const qwenRows = [];
  const explicitQwenOnly = normalizeOcrProvider(config?.ocrProvider) === "qwen";

  if (parsedRows.length === 0 && config.ocrEnabled && Array.isArray(post.images) && post.images.length > 0) {
    const maxImages = Math.min(config.ocrMaxImagesPerPost, post.images.length);

    for (let i = 0; i < maxImages; i += 1) {
      const imageUrl = post.images[i];
      if (shouldPreferQwenOcr(config)) {
        let nativeRowsFound = false;
        let shouldTryCompatible = true;

        try {
          const nativeQwenResult = await extractRowsWithQwenNativeOcr(imageUrl, post, config);
          if (nativeQwenResult.rawText) {
            ocrText += `\n${nativeQwenResult.rawText}`;
          }
          if (Array.isArray(nativeQwenResult.rows) && nativeQwenResult.rows.length > 0) {
            qwenRows.push(...nativeQwenResult.rows);
            nativeRowsFound = true;
            shouldTryCompatible = shouldRunQwenCompatibleFallback(nativeQwenResult.rows);
          }
        } catch {
          // Ignore native OCR failure and continue to fallback strategies.
        }

        if (shouldTryCompatible) {
          try {
            const compatibleQwenResult = await extractRowsWithQwenCompatibleOcr(imageUrl, post, config);
            if (compatibleQwenResult.rawText) {
              ocrText += `\n${compatibleQwenResult.rawText}`;
            }
            if (Array.isArray(compatibleQwenResult.rows) && compatibleQwenResult.rows.length > 0) {
              qwenRows.push(...compatibleQwenResult.rows);
              nativeRowsFound = true;
            }
          } catch {
            // Ignore remote OCR failure and fall back to local OCR.
          }
        }

        if (nativeRowsFound) {
          continue;
        }
      }

      if (explicitQwenOnly) {
        continue;
      }

      try {
        const text = await extractTextWithOcr(imageUrl, post, {
          ...config,
          ocrProvider: "local"
        });
        if (!text || !text.trim()) {
          continue;
        }
        ocrText += `\n${text}`;
      } catch {
        continue;
      }
    }

    const localRows = ocrText.trim() ? extractRowsFromText(ocrText) : [];
    if (qwenRows.length > 0 || localRows.length > 0) {
      parsedRows = dedupeSnapshotRows([...qwenRows, ...localRows]);
    }
  }

  if (parsedRows.length === 0) {
    return null;
  }

  return {
    source: post.source,
    postId: post.postId,
    postedAt: post.postedAt,
    link: post.link,
    title: post.title || "",
    rows: parsedRows,
    rawText: stripHtml(post.text),
    ocrText: ocrText.trim(),
    images: Array.isArray(post.images) ? post.images : []
  };
}

async function collectBackfillCandidates(config, addLog, options = {}) {
  const candidates = [];
  const xqCookieState = getCookieState(config.xueqiuCookie);
  if (xqCookieState !== "ok") {
    addLog("warn", `${cookieWarnText("雪球", xqCookieState)}，无法执行历史回溯`);
    return candidates;
  }

  const backfillPages = clampNumber(
    options.backfillPages,
    config.backfillMaxPages,
    1,
    120
  );
  const pageSize = clampNumber(
    options.backfillPageSize,
    config.backfillPageSize,
    5,
    50
  );

  try {
    const posts = await fetchXueqiuPosts(config, {
      pageFrom: 1,
      pageTo: backfillPages,
      pageSize
    });

    addLog("info", `历史回溯拉取完成：共 ${posts.length} 条雪球帖子（${backfillPages} 页内）`);

    const titleRegex = buildXueqiuTitleRegex(config);
    const targetPosts = posts.filter((post) => isXueqiuTargetTitlePost(post, titleRegex));
    addLog("info", `标题匹配「${config.xueqiuTitleRegex}」命中: ${targetPosts.length} 条`);

    candidates.push(...targetPosts);
  } catch (error) {
    addLog("error", `历史回溯拉取失败: ${error.message}`);
  }

  return candidates;
}

async function collectNormalCandidates(config, addLog) {
  const candidates = [];

  if (config.pinnedPostUrls.length > 0) {
    const pinnedPosts = await fetchPinnedPosts(config, addLog);
    if (pinnedPosts.length > 0) {
      addLog("info", `置顶链接拉取成功: ${pinnedPosts.length} 条`);
      candidates.push(...pinnedPosts);
    } else {
      addLog("warn", "置顶链接未抓取到有效帖子");
    }
  } else {
    addLog("warn", "未配置置顶链接，已回退时间线抓取");
  }

  const xqCookieState = getCookieState(config.xueqiuCookie);
  if (xqCookieState !== "ok") {
    addLog("warn", `${cookieWarnText("雪球", xqCookieState)}，已跳过时间线抓取`);
  } else {
    try {
      const posts = await fetchXueqiuPosts(config, {
        pageFrom: 1,
        pageTo: 1,
        pageSize: config.maxPostsPerSource,
        maxTotal: config.maxPostsPerSource
      });
      addLog("info", `雪球时间线拉取成功: ${posts.length} 条`);
      candidates.push(...posts);
    } catch (error) {
      addLog("error", `雪球时间线拉取失败: ${error.message}`);
    }
  }

  const wbCookieState = getCookieState(config.weiboCookie);
  if (wbCookieState !== "ok") {
    addLog("warn", `${cookieWarnText("微博", wbCookieState)}，已跳过时间线抓取`);
  } else {
    try {
      const posts = await fetchWeiboPosts(config);
      addLog("info", `微博时间线拉取成功: ${posts.length} 条`);
      candidates.push(...posts);
    } catch (error) {
      addLog("error", `微博时间线拉取失败: ${error.message}`);
    }
  }

  return candidates;
}

async function collectTargetCandidates(config, targetPostIds, addLog, options = {}) {
  const candidates = [];
  const allIds = Array.isArray(targetPostIds) ? targetPostIds : [...targetPostIds];
  const xueqiuIds = [];
  const weiboIds = [];

  for (const rawId of allIds) {
    const postId = String(rawId || "").trim();
    if (/^xq:\d{6,}$/i.test(postId)) {
      xueqiuIds.push(postId.slice(3));
      continue;
    }
    if (/^wb:[A-Za-z0-9]{6,}$/i.test(postId)) {
      weiboIds.push(postId.slice(3));
    }
  }

  if (xueqiuIds.length > 0) {
    const xqCookieState = getCookieState(config.xueqiuCookie);
    if (xqCookieState !== "ok") {
      addLog("warn", `${cookieWarnText("雪球", xqCookieState)}，已跳过指定雪球帖子抓取`);
    } else {
      const backfillPages = clampNumber(
        options.backfillPages,
        config.backfillMaxPages,
        1,
        120
      );
      const pageSize = clampNumber(
        options.backfillPageSize,
        config.backfillPageSize,
        5,
        50
      );
      const requestedSet = new Set(xueqiuIds.map((item) => String(item)));
      const matchedIds = new Set();

      try {
        const timelinePosts = await fetchXueqiuPosts(config, {
          pageFrom: 1,
          pageTo: backfillPages,
          pageSize
        });
        const matchedPosts = timelinePosts.filter((post) => requestedSet.has(String(post.postId || "").replace(/^xq:/i, "")));
        for (const post of matchedPosts) {
          matchedIds.add(String(post.postId || "").replace(/^xq:/i, ""));
          addLog("info", `指定帖子命中历史时间线（雪球）: ${String(post.postId || "").replace(/^xq:/i, "")}`);
        }
        candidates.push(...matchedPosts);
        addLog("info", `指定帖子历史时间线扫描完成（雪球）: 请求 ${xueqiuIds.length} 条，命中 ${matchedPosts.length} 条`);
      } catch (error) {
        addLog("error", `指定帖子历史时间线扫描失败（雪球）: ${error.message}`);
      }

      const remainingIds = xueqiuIds.filter((postId) => !matchedIds.has(String(postId)));
      if (remainingIds.length > 0) {
        const posts = await mapWithConcurrency(remainingIds, 3, async (postId) => {
          try {
            const post = await fetchXueqiuPostById(postId, config, null);
            addLog("info", `指定帖子抓取成功（雪球详情兜底）: ${postId}`);
            return post;
          } catch (error) {
            addLog("error", `指定帖子抓取失败（雪球）: ${postId} | ${error.message}`);
            return null;
          }
        });
        candidates.push(...posts.filter(Boolean));
      }
    }
  }

  if (weiboIds.length > 0) {
    const wbCookieState = getCookieState(config.weiboCookie);
    if (wbCookieState !== "ok") {
      addLog("warn", `${cookieWarnText("微博", wbCookieState)}，已跳过指定微博帖子抓取`);
    } else {
      const posts = await mapWithConcurrency(weiboIds, 3, async (postId) => {
        try {
          const post = await fetchWeiboPostById(postId, config, null);
          addLog("info", `指定帖子抓取成功（微博）: ${postId}`);
          return post;
        } catch (error) {
          addLog("error", `指定帖子抓取失败（微博）: ${postId} | ${error.message}`);
          return null;
        }
      });
      candidates.push(...posts.filter(Boolean));
    }
  }

  addLog("info", `按选择帖子抓取完成：请求 ${allIds.length} 条，成功 ${candidates.length} 条`);
  return candidates;
}

async function collectSuperLudinggongSnapshots(inputConfig, processedPostIds = [], options = {}) {
  const config = mergeAutoTrackingConfig(inputConfig);
  const processedSet = new Set(processedPostIds);
  const targetPostIds = new Set(
    Array.isArray(options.targetPostIds)
      ? options.targetPostIds.map((item) => String(item || "").trim()).filter(Boolean)
      : []
  );
  const hasTargetPostIds = targetPostIds.size > 0;
  const mode = options.mode === "backfill" ? "backfill" : "normal";

  const logs = [];
  const addLog = (level, message, meta = null) => {
    logs.push({
      id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      level,
      message,
      meta
    });
  };

  const rawCandidates =
    mode === "backfill"
      ? hasTargetPostIds
        ? await collectTargetCandidates(config, [...targetPostIds], addLog, options)
        : await collectBackfillCandidates(config, addLog, options)
      : await collectNormalCandidates(config, addLog);

  const titleRegex = buildXueqiuTitleRegex(config);
  const sorted = dedupePostsById(rawCandidates).sort((a, b) => {
    const aTime = new Date(a.postedAt).getTime();
    const bTime = new Date(b.postedAt).getTime();
    return bTime - aTime;
  });

  const parseCandidates = [];
  let filteredByTitle = 0;

  for (const post of sorted) {
    if (hasTargetPostIds && !targetPostIds.has(post.postId)) {
      continue;
    }

    if (processedSet.has(post.postId)) {
      continue;
    }

    const titleMatched = isXueqiuTargetTitlePost(post, titleRegex);
    if (mode === "backfill" && !hasTargetPostIds && post.source === "xueqiu" && !titleMatched) {
      filteredByTitle += 1;
      continue;
    }

    const shouldTryParse =
      post.source === "xueqiu"
        ? post.fromPinned || titleMatched || hasTargetPostIds
        : post.fromPinned || isLikelyHoldingPost(post, config.keywords) || (Array.isArray(post.images) && post.images.length > 0);

    if (!shouldTryParse) {
      continue;
    }
    parseCandidates.push(post);
  }

  if (parseCandidates.length > 1) {
    addLog("info", `开始解析候选帖子: ${parseCandidates.length} 条，并发 ${SNAPSHOT_PARSE_CONCURRENCY}`);
  }

  const snapshots = (
    await mapWithConcurrency(parseCandidates, SNAPSHOT_PARSE_CONCURRENCY, async (post) => {
      try {
        return await parseSnapshotFromPost(post, config);
      } catch (error) {
        addLog("warn", `帖子解析失败，已跳过: ${post.postId} | ${error.message}`);
        return null;
      }
    })
  ).filter(Boolean);

  if (hasTargetPostIds && snapshots.length === 0) {
    addLog("warn", `选择导入的帖子未识别到可用持仓：${targetPostIds.size} 条`);
  }

  if (mode === "backfill" && filteredByTitle > 0) {
    addLog("info", `回溯过滤：因标题不匹配跳过 ${filteredByTitle} 条`);
  }

  addLog("info", `识别到可导入快照: ${snapshots.length} 条`);

  return {
    snapshots,
    logs,
    config
  };
}

async function collectSuperLudinggongPostCatalog(inputConfig, options = {}) {
  const config = mergeAutoTrackingConfig(inputConfig);
  const logs = [];
  const addLog = (level, message, meta = null) => {
    logs.push({
      id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
      createdAt: new Date().toISOString(),
      level,
      message,
      meta
    });
  };

  const rawCandidates = await collectBackfillCandidates(config, addLog, options);
  const titleRegex = buildXueqiuTitleRegex(config);

  const posts = dedupePostsById(rawCandidates)
    .filter((post) => isXueqiuTargetTitlePost(post, titleRegex))
    .sort((a, b) => {
      const aTime = new Date(a.postedAt).getTime();
      const bTime = new Date(b.postedAt).getTime();
      return bTime - aTime;
    })
    .map((post) => ({
      postId: post.postId,
      source: post.source,
      title: post.title || "",
      postedAt: post.postedAt,
      link: post.link,
      imageCount: Array.isArray(post.images) ? post.images.length : 0,
      fromPinned: Boolean(post.fromPinned)
    }));

  addLog("info", `目录可选帖子: ${posts.length} 条`);

  return {
    posts,
    logs,
    config
  };
}

module.exports = {
  DEFAULT_AUTO_TRACKING,
  ensureAutoTrackingState,
  mergeAutoTrackingConfig,
  collectSuperLudinggongSnapshots,
  collectSuperLudinggongPostCatalog
};
