const path = require("node:path");
const { createHash, createHmac, randomUUID, timingSafeEqual } = require("node:crypto");

const express = require("express");
const appPackage = require("../package.json");

const { ensureStore, readStore, mutateStore, getStoreHealthSummary } = require("./store");
const { buildPortfolio, buildMonthlyStatus, toMonthKey } = require("./portfolio");
const { extractSnapshotPostMetrics } = require("./post-metrics");
const { refreshQuotes, lookupSecurityNames } = require("./quotes");
const { toApiSymbol, normalizeMarket, normalizeSecurityName } = require("./symbols");
const {
  ensureAutoTrackingState,
  mergeAutoTrackingConfig,
  collectSuperLudinggongSnapshots,
  collectSuperLudinggongPostCatalog,
  keepCookiesAlive
} = require("./super-ludinggong-sync");
const {
  createJob,
  startJob,
  finishJob,
  failJob,
  skipJob,
  getJob,
  getJobOverview
} = require("./job-state");
const {
  buildAutoTrackingConfigPatch,
  summarizeAutoTrackingResult,
  normalizeBackfillInput,
  normalizeSelectedImportInput,
  normalizePostIds: normalizeRequestedPostIds,
  classifyAutoTrackingResult
} = require("./auto-tracking-service");

const app = express();
const PORT = Number(process.env.PORT) || 8787;
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();
const ADMIN_AUTH_ENABLED = ADMIN_PASSWORD.length > 0;
const ADMIN_SESSION_COOKIE = "stock_lu_admin";
const ADMIN_SESSION_TTL_HOURS = Math.max(1, Number(process.env.ADMIN_SESSION_TTL_HOURS) || 24);
const ADMIN_SESSION_TTL_MS = ADMIN_SESSION_TTL_HOURS * 60 * 60 * 1000;
const ADMIN_COOKIE_SECURE = ["1", "true", "yes", "on"].includes(
  String(process.env.ADMIN_COOKIE_SECURE || "").trim().toLowerCase()
);
const ADMIN_SESSION_SECRET = createHash("sha256")
  .update(`stock-lu-admin:${ADMIN_PASSWORD}:${process.pid}:${Date.now()}`)
  .digest("hex");

const PROFILE_LINKS = {
  xueqiu: "https://xueqiu.com/u/8790885129",
  weibo: "https://weibo.com/u/3962719063"
};

function normalizeRepositoryUrl(value) {
  return String(value || "")
    .trim()
    .replace(/^git\+/, "")
    .replace(/\.git$/i, "");
}

const APP_META = Object.freeze({
  productName: "超级鹿鼎公持仓自动跟踪",
  version: String(appPackage.version || "0.0.0").trim() || "0.0.0",
  versionLabel: `v${String(appPackage.version || "0.0.0").trim() || "0.0.0"}`,
  author: String(appPackage.author || "Kale").trim() || "Kale",
  repositoryUrl: normalizeRepositoryUrl(appPackage.homepage || appPackage.repository?.url),
  repositoryLabel: "icekale/stock-lu-tracker"
});
const CATALOG_CACHE_TTL_MS = Math.max(0, Number(process.env.CATALOG_CACHE_TTL_MS) || 5 * 60 * 1000);
const AUTO_TRACKING_TIME_ZONE = String(process.env.AUTO_TRACKING_TIME_ZONE || "Asia/Shanghai").trim() || "Asia/Shanghai";
const AUTO_TRACKING_MIN_DELAY_MS = 60 * 1000;

let autoTrackingRunning = false;
let autoTrackingTimer = null;
let cookieKeepAliveRunning = false;
let cookieKeepAliveTimer = null;
const catalogCache = new Map();
const catalogInFlight = new Map();

app.use(express.json({ limit: "1mb" }));

function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function toDateIso(value) {
  const date = value ? new Date(value) : new Date();
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function isSampleCookie(cookie) {
  const text = String(cookie || "").trim();
  if (!text) {
    return false;
  }
  const samples = ["abc123", "xyz987", "_2A25Labcde", "Hm_lvt_test"];
  return samples.some((item) => text.includes(item));
}

function createHttpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function setNoStoreHeaders(res) {
  res.set("Cache-Control", "no-store, max-age=0");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
}

function setPublicAssetCacheHeaders(res, filePath) {
  const normalizedPath = String(filePath || "").replace(/\\/g, "/");
  if (normalizedPath.endsWith(".html") || normalizedPath.endsWith("/site-footer.js")) {
    setNoStoreHeaders(res);
  }
}

function parseCookies(cookieHeader) {
  const cookies = {};
  const raw = String(cookieHeader || "");
  if (!raw) {
    return cookies;
  }

  for (const segment of raw.split(";")) {
    const trimmed = segment.trim();
    if (!trimmed) {
      continue;
    }
    const [key, ...rest] = trimmed.split("=");
    if (!key) {
      continue;
    }
    const valueRaw = rest.join("=");
    try {
      cookies[key] = decodeURIComponent(valueRaw);
    } catch (_error) {
      cookies[key] = valueRaw;
    }
  }

  return cookies;
}

function hashText(value) {
  return createHash("sha256").update(String(value || "")).digest();
}

function toTrimmedText(value) {
  return String(value || "").trim();
}

function toOptionalNumber(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function getTimeZoneDateParts(date, timeZone = AUTO_TRACKING_TIME_ZONE) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  const parts = {};

  for (const part of formatter.formatToParts(date)) {
    if (part.type === "literal") {
      continue;
    }
    parts[part.type] = Number(part.value);
  }

  return {
    year: parts.year || 1970,
    month: parts.month || 1,
    day: parts.day || 1,
    hour: parts.hour || 0,
    minute: parts.minute || 0,
    second: parts.second || 0
  };
}

function getTimeZoneOffsetMs(date, timeZone = AUTO_TRACKING_TIME_ZONE) {
  const parts = getTimeZoneDateParts(date, timeZone);
  const utcTimestamp = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return utcTimestamp - date.getTime();
}

function zonedTimeToUtc(year, month, day, hour = 0, minute = 0, second = 0, timeZone = AUTO_TRACKING_TIME_ZONE) {
  const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  const offsetMs = getTimeZoneOffsetMs(utcGuess, timeZone);
  return new Date(utcGuess.getTime() - offsetMs);
}

function getDaysInMonth(year, month) {
  return new Date(Date.UTC(year, month, 0)).getUTCDate();
}

function shiftMonth(year, month, offset = 1) {
  const absolute = year * 12 + (month - 1) + offset;
  const nextYear = Math.floor(absolute / 12);
  const nextMonth = ((absolute % 12) + 12) % 12 + 1;
  return {
    year: nextYear,
    month: nextMonth
  };
}

function formatScheduleDateTime(date, timeZone = AUTO_TRACKING_TIME_ZONE) {
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone,
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).format(date);
}

function getMonthEndWindowState(now, windowDays, timeZone = AUTO_TRACKING_TIME_ZONE) {
  const parts = getTimeZoneDateParts(now, timeZone);
  const daysInMonth = getDaysInMonth(parts.year, parts.month);
  const startDay = Math.max(1, daysInMonth - windowDays + 1);
  const inWindow = parts.day >= startDay;
  const currentWindowStartAt = zonedTimeToUtc(parts.year, parts.month, startDay, 0, 0, 0, timeZone);

  if (!inWindow) {
    return {
      inWindow,
      nextWindowStartAt: currentWindowStartAt
    };
  }

  const nextMonth = shiftMonth(parts.year, parts.month, 1);
  const nextMonthDays = getDaysInMonth(nextMonth.year, nextMonth.month);
  const nextStartDay = Math.max(1, nextMonthDays - windowDays + 1);

  return {
    inWindow,
    nextWindowStartAt: zonedTimeToUtc(nextMonth.year, nextMonth.month, nextStartDay, 0, 0, 0, timeZone)
  };
}

function buildAutoTrackingSchedule(configInput, now = new Date()) {
  const config = mergeAutoTrackingConfig(configInput);
  const intervalMs = config.intervalMinutes * 60 * 1000;

  if (!config.enabled) {
    return {
      mode: "disabled",
      delayMs: null,
      nextRunAt: null,
      scheduleHint: "自动同步已关闭",
      idleIntervalMs: config.offWindowIntervalHours * 60 * 60 * 1000
    };
  }

  if (!config.smartScheduleEnabled) {
    return {
      mode: "fixed_interval",
      delayMs: intervalMs,
      nextRunAt: new Date(now.getTime() + intervalMs),
      scheduleHint: `固定每 ${config.intervalMinutes} 分钟抓取一次`,
      idleIntervalMs: config.offWindowIntervalHours * 60 * 60 * 1000
    };
  }

  const windowState = getMonthEndWindowState(now, config.monthEndWindowDays, AUTO_TRACKING_TIME_ZONE);
  if (windowState.inWindow) {
    return {
      mode: "month_end_window",
      delayMs: intervalMs,
      nextRunAt: new Date(now.getTime() + intervalMs),
      scheduleHint: `月底最后 ${config.monthEndWindowDays} 天高频抓取，每 ${config.intervalMinutes} 分钟一次`,
      idleIntervalMs: config.offWindowIntervalHours * 60 * 60 * 1000
    };
  }

  const idleIntervalMs = config.offWindowIntervalHours * 60 * 60 * 1000;
  const idleCandidateAt = new Date(now.getTime() + idleIntervalMs);
  const waitForWindow = idleCandidateAt.getTime() >= windowState.nextWindowStartAt.getTime();
  const nextRunAt = waitForWindow ? windowState.nextWindowStartAt : idleCandidateAt;

  return {
    mode: "off_window",
    delayMs: Math.max(AUTO_TRACKING_MIN_DELAY_MS, nextRunAt.getTime() - now.getTime()),
    nextRunAt,
    scheduleHint: waitForWindow
      ? `非月底低频巡检，等待至北京时间 ${formatScheduleDateTime(nextRunAt)} 进入月底窗口`
      : `非月底低频巡检，每 ${config.offWindowIntervalHours} 小时检查一次`,
    idleIntervalMs,
    waitForWindow
  };
}

function shouldRunAutoTrackingOnStartup(autoTrackingInput, now = new Date()) {
  const autoTracking = autoTrackingInput || {};
  const config = mergeAutoTrackingConfig(autoTracking.config);
  const schedule = buildAutoTrackingSchedule(config, now);

  if (!config.enabled) {
    return {
      shouldRun: false,
      reason: "disabled",
      schedule
    };
  }

  if (!config.smartScheduleEnabled || !config.skipStartupOutsideWindow || schedule.mode !== "off_window") {
    return {
      shouldRun: true,
      reason: "startup_allowed",
      schedule
    };
  }

  const lastActivityRaw = autoTracking.runtime?.lastSuccessAt || autoTracking.runtime?.lastRunAt || null;
  if (!lastActivityRaw) {
    return {
      shouldRun: true,
      reason: "no_previous_run",
      schedule
    };
  }

  const lastActivityAt = new Date(lastActivityRaw);
  if (Number.isNaN(lastActivityAt.getTime())) {
    return {
      shouldRun: true,
      reason: "invalid_previous_run",
      schedule
    };
  }

  const shouldRun = now.getTime() - lastActivityAt.getTime() >= schedule.idleIntervalMs;
  return {
    shouldRun,
    reason: shouldRun ? "idle_interval_elapsed" : "outside_month_end_window",
    schedule
  };
}

function buildCookieKeepAliveSchedule(configInput, runtimeInput = {}, now = new Date()) {
  const config = mergeAutoTrackingConfig(configInput);
  const runtime = runtimeInput || {};

  if (!config.cookieKeepAliveEnabled) {
    return {
      mode: "disabled",
      delayMs: null,
      nextRunAt: null,
      scheduleHint: "Cookie 保活已关闭"
    };
  }

  const intervalMs = config.cookieKeepAliveIntervalHours * 60 * 60 * 1000;
  const lastActivityRaw = runtime.lastCookieKeepAliveSuccessAt || runtime.lastCookieKeepAliveAt || null;
  if (!lastActivityRaw) {
    return {
      mode: "initial",
      delayMs: AUTO_TRACKING_MIN_DELAY_MS,
      nextRunAt: new Date(now.getTime() + AUTO_TRACKING_MIN_DELAY_MS),
      scheduleHint: `Cookie 保活已开启，每 ${config.cookieKeepAliveIntervalHours} 小时执行一次`
    };
  }

  const lastActivityAt = new Date(lastActivityRaw);
  if (Number.isNaN(lastActivityAt.getTime())) {
    return {
      mode: "initial",
      delayMs: AUTO_TRACKING_MIN_DELAY_MS,
      nextRunAt: new Date(now.getTime() + AUTO_TRACKING_MIN_DELAY_MS),
      scheduleHint: `Cookie 保活已开启，每 ${config.cookieKeepAliveIntervalHours} 小时执行一次`
    };
  }

  const nextDueAt = new Date(lastActivityAt.getTime() + intervalMs);
  if (nextDueAt.getTime() <= now.getTime()) {
    return {
      mode: "overdue",
      delayMs: AUTO_TRACKING_MIN_DELAY_MS,
      nextRunAt: new Date(now.getTime() + AUTO_TRACKING_MIN_DELAY_MS),
      scheduleHint: `Cookie 保活已到期，准备执行（间隔 ${config.cookieKeepAliveIntervalHours} 小时）`,
      overdue: true
    };
  }

  return {
    mode: "scheduled",
    delayMs: Math.max(AUTO_TRACKING_MIN_DELAY_MS, nextDueAt.getTime() - now.getTime()),
    nextRunAt: nextDueAt,
    scheduleHint: `Cookie 保活每 ${config.cookieKeepAliveIntervalHours} 小时执行一次`
  };
}

function shouldRunCookieKeepAliveOnStartup(autoTrackingInput, now = new Date()) {
  const autoTracking = autoTrackingInput || {};
  const schedule = buildCookieKeepAliveSchedule(autoTracking.config, autoTracking.runtime, now);
  return {
    shouldRun: schedule.mode === "initial" || schedule.mode === "overdue",
    reason: schedule.mode,
    schedule
  };
}

function buildCatalogCacheKey(config, options = {}) {
  return JSON.stringify({
    xueqiuCookieHash: toTrimmedText(config?.xueqiuCookie)
      ? createHash("sha256").update(toTrimmedText(config.xueqiuCookie)).digest("hex").slice(0, 16)
      : "",
    titleRegex: toTrimmedText(config?.xueqiuTitleRegex),
    backfillPages: toOptionalNumber(options.backfillPages, toOptionalNumber(config?.backfillMaxPages)),
    backfillPageSize: toOptionalNumber(options.backfillPageSize, toOptionalNumber(config?.backfillPageSize))
  });
}

function invalidateCatalogCache() {
  catalogCache.clear();
}

async function collectSuperLudinggongPostCatalogCached(config, options = {}) {
  const cacheKey = buildCatalogCacheKey(config, options);
  const now = Date.now();

  if (CATALOG_CACHE_TTL_MS > 0) {
    const cached = catalogCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return {
        catalog: structuredClone(cached.catalog),
        cacheHit: true
      };
    }
  }

  if (catalogInFlight.has(cacheKey)) {
    const sharedCatalog = await catalogInFlight.get(cacheKey);
    return {
      catalog: structuredClone(sharedCatalog),
      cacheHit: true
    };
  }

  const task = collectSuperLudinggongPostCatalog(config, options)
    .then((catalog) => {
      const snapshot = structuredClone(catalog);
      if (CATALOG_CACHE_TTL_MS > 0) {
        catalogCache.set(cacheKey, {
          catalog: snapshot,
          expiresAt: Date.now() + CATALOG_CACHE_TTL_MS
        });
      }
      return snapshot;
    })
    .finally(() => {
      catalogInFlight.delete(cacheKey);
    });

  catalogInFlight.set(cacheKey, task);

  const catalog = await task;
  return {
    catalog: structuredClone(catalog),
    cacheHit: false
  };
}

function isAdminPasswordMatch(inputPassword) {
  if (!ADMIN_AUTH_ENABLED) {
    return true;
  }
  return timingSafeEqual(hashText(inputPassword), hashText(ADMIN_PASSWORD));
}

function createAdminSessionToken() {
  const payload = {
    exp: Date.now() + ADMIN_SESSION_TTL_MS,
    nonce: randomUUID()
  };
  const payloadEncoded = Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
  const signature = createHmac("sha256", ADMIN_SESSION_SECRET).update(payloadEncoded).digest("base64url");
  return `${payloadEncoded}.${signature}`;
}

function verifyAdminSessionToken(token) {
  if (!ADMIN_AUTH_ENABLED) {
    return true;
  }

  const raw = String(token || "").trim();
  if (!raw.includes(".")) {
    return false;
  }

  const [payloadEncoded, signatureEncoded] = raw.split(".");
  if (!payloadEncoded || !signatureEncoded) {
    return false;
  }

  let payload = null;
  let expectedSignature;
  let providedSignature;

  try {
    payload = JSON.parse(Buffer.from(payloadEncoded, "base64url").toString("utf8"));
    expectedSignature = createHmac("sha256", ADMIN_SESSION_SECRET).update(payloadEncoded).digest();
    providedSignature = Buffer.from(signatureEncoded, "base64url");
  } catch (_error) {
    return false;
  }

  if (!Buffer.isBuffer(expectedSignature) || !Buffer.isBuffer(providedSignature)) {
    return false;
  }

  if (expectedSignature.length !== providedSignature.length) {
    return false;
  }

  if (!timingSafeEqual(expectedSignature, providedSignature)) {
    return false;
  }

  const expiresAt = Number(payload?.exp) || 0;
  if (expiresAt <= Date.now()) {
    return false;
  }

  return true;
}

function buildCookieSecureAttr() {
  return ADMIN_COOKIE_SECURE ? "; Secure" : "";
}

function setAdminSessionCookie(res, token) {
  const maxAge = Math.max(60, Math.floor(ADMIN_SESSION_TTL_MS / 1000));
  const secure = buildCookieSecureAttr();
  const value = encodeURIComponent(String(token || ""));
  res.setHeader(
    "Set-Cookie",
    `${ADMIN_SESSION_COOKIE}=${value}; Path=/; Max-Age=${maxAge}; HttpOnly; SameSite=Lax${secure}`
  );
}

function clearAdminSessionCookie(res) {
  const secure = buildCookieSecureAttr();
  res.setHeader(
    "Set-Cookie",
    `${ADMIN_SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax${secure}`
  );
}

function isAdminAuthenticated(req) {
  if (!ADMIN_AUTH_ENABLED) {
    return true;
  }
  const cookies = parseCookies(req.headers?.cookie);
  return verifyAdminSessionToken(cookies[ADMIN_SESSION_COOKIE]);
}

function requireAdminAuth(req, res, next) {
  if (!ADMIN_AUTH_ENABLED) {
    next();
    return;
  }

  if (isAdminAuthenticated(req)) {
    next();
    return;
  }

  const isApiRequest = String(req.originalUrl || req.path || "").startsWith("/api/");
  if (isApiRequest) {
    res.status(401).json({ error: "后台未登录，请先输入管理密码" });
    return;
  }

  const nextPath = encodeURIComponent(req.originalUrl || "/admin.html");
  res.redirect(302, `/admin-login.html?next=${nextPath}`);
}

app.get("/api/admin-auth/status", (req, res) => {
  res.json({
    enabled: ADMIN_AUTH_ENABLED,
    authenticated: isAdminAuthenticated(req),
    sessionTtlHours: ADMIN_SESSION_TTL_HOURS
  });
});

app.post("/api/admin-auth/login", (req, res, next) => {
  try {
    if (!ADMIN_AUTH_ENABLED) {
      res.json({
        ok: true,
        enabled: false
      });
      return;
    }

    const password = String(req.body?.password || "");
    if (!isAdminPasswordMatch(password)) {
      throw createHttpError(401, "密码错误");
    }

    const token = createAdminSessionToken();
    setAdminSessionCookie(res, token);

    res.json({
      ok: true,
      enabled: true
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/admin-auth/logout", (_req, res) => {
  clearAdminSessionCookie(res);
  res.json({ ok: true });
});

app.get("/admin.html", requireAdminAuth, (_req, res) => {
  setNoStoreHeaders(res);
  res.sendFile(path.join(process.cwd(), "public", "admin.html"));
});

app.get("/auto-sync", (_req, res) => {
  res.redirect(302, "/admin.html");
});

app.use("/api/state", requireAdminAuth);
app.use("/api/trades", requireAdminAuth);
app.use("/api/quotes", requireAdminAuth);
app.use("/api/snapshots", requireAdminAuth);
app.use("/api/monthly-updates", requireAdminAuth);
app.use("/api/auto-tracking", requireAdminAuth);
app.use("/api/jobs", requireAdminAuth);
app.use("/vendor/layui", express.static(path.join(process.cwd(), "node_modules", "layui", "dist")));
app.use(
  express.static(path.join(process.cwd(), "public"), {
    setHeaders: setPublicAssetCacheHeaders
  })
);

function pushSnapshot(store, source = "manual") {
  const { summary } = buildPortfolio(store.trades, store.quotes);

  const snapshot = {
    id: randomUUID(),
    timestamp: new Date().toISOString(),
    source,
    totalMarketValue: summary.totalMarketValue,
    totalCost: summary.totalCost,
    totalUnrealizedPnl: summary.totalUnrealizedPnl,
    totalDailyPnl: summary.totalDailyPnl,
    holdingCount: summary.holdingCount
  };

  store.snapshots.push(snapshot);

  if (store.snapshots.length > 5000) {
    store.snapshots = store.snapshots.slice(-5000);
  }

  return snapshot;
}

function buildState(store) {
  const portfolio = buildPortfolio(store.trades, store.quotes);
  const monthlyStatus = buildMonthlyStatus(store.monthlyUpdates || []);
  const autoTracking = ensureAutoTrackingState(store);
  const latestMasterSnapshot = sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null);

  return {
    summary: portfolio.summary,
    positions: portfolio.positions,
    monthlyStatus,
    autoTracking: getAutoTrackingPublic(autoTracking),
    latestMasterSnapshot
  };
}

function parseTradeInput(payload) {
  const symbol = String(payload.symbol || "").trim().toUpperCase();
  if (!symbol) {
    throw createHttpError(400, "symbol 不能为空");
  }

  const type = String(payload.type || "BUY").toUpperCase();
  if (!["BUY", "SELL"].includes(type)) {
    throw createHttpError(400, "type 只能是 BUY 或 SELL");
  }

  const quantity = toNumber(payload.quantity);
  if (!quantity || quantity <= 0) {
    throw createHttpError(400, "quantity 必须大于 0");
  }

  const price = toNumber(payload.price);
  if (!price || price <= 0) {
    throw createHttpError(400, "price 必须大于 0");
  }

  const feeRaw = payload.fee === "" || payload.fee === null || typeof payload.fee === "undefined" ? 0 : toNumber(payload.fee);
  if (feeRaw === null || feeRaw < 0) {
    throw createHttpError(400, "fee 不能小于 0");
  }

  const market = normalizeMarket(symbol, payload.market);
  const apiSymbol = toApiSymbol(symbol, market);
  if (!apiSymbol) {
    throw createHttpError(400, "无法识别股票代码");
  }

  const tradeDate = toDateIso(payload.tradeDate);
  if (!tradeDate) {
    throw createHttpError(400, "tradeDate 无效");
  }

  return {
    id: randomUUID(),
    symbol,
    apiSymbol,
    market,
    name: normalizeSecurityName(symbol, payload.name, market, {
      nameSource: payload.nameSource
    }),
    nameSource: String(payload.nameSource || "").trim().toLowerCase(),
    type,
    quantity,
    price,
    fee: feeRaw,
    tradeDate,
    note: String(payload.note || "").trim(),
    createdAt: new Date().toISOString()
  };
}

function normalizeSourceSymbol(rawSymbol) {
  const value = String(rawSymbol || "").trim().toUpperCase();
  if (!value || value.includes("CASH")) {
    return null;
  }

  if (/^\d{6}\.(SH|SZ)$/.test(value)) {
    return {
      symbol: value.slice(0, 6),
      market: "CN"
    };
  }

  if (/^\d{4,5}\.HK$/.test(value)) {
    return {
      symbol: value.replace(".HK", ""),
      market: "HK"
    };
  }

  if (/^\d{6}$/.test(value)) {
    return {
      symbol: value,
      market: "CN"
    };
  }

  if (/^\d{4,5}$/.test(value)) {
    return {
      symbol: value,
      market: "HK"
    };
  }

  if (/^[A-Z]{1,6}(\.[A-Z]{2,3})?$/.test(value)) {
    return {
      symbol: value.split(".")[0],
      market: "US"
    };
  }

  return null;
}

function collectSnapshotApiSymbols(snapshots) {
  const apiSymbols = new Set();

  for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
    for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
      const normalized = normalizeSourceSymbol(row?.symbol);
      if (!normalized) {
        continue;
      }
      const apiSymbol = toApiSymbol(normalized.symbol, normalized.market);
      if (apiSymbol) {
        apiSymbols.add(apiSymbol);
      }
    }
  }

  return [...apiSymbols];
}

function applyCollectedSecurityNameValidation(snapshots, namesBySymbol, sourcesBySymbol = {}) {
  if (!namesBySymbol || typeof namesBySymbol !== "object") {
    return;
  }

  for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
    for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
      const normalized = normalizeSourceSymbol(row?.symbol);
      if (!normalized) {
        continue;
      }

      const apiSymbol = toApiSymbol(normalized.symbol, normalized.market);
      const validatedName = String(namesBySymbol[apiSymbol] || "").trim();
      const nameSource = String(sourcesBySymbol[apiSymbol] || "").trim().toLowerCase();
      if (!validatedName) {
        continue;
      }

      row.name = normalizeSecurityName(normalized.symbol, validatedName, normalized.market, {
        nameSource
      });
      row.nameSource = nameSource;
    }
  }
}

function toFiniteNumber(value) {
  if (value === null || typeof value === "undefined") {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function toPositiveNumber(value) {
  const number = toFiniteNumber(value);
  return number !== null && number > 0 ? number : null;
}

function roundNumeric(value, digits = 3) {
  const number = toFiniteNumber(value);
  if (number === null) {
    return null;
  }
  const factor = 10 ** digits;
  return Math.round(number * factor) / factor;
}

function numericGap(left, right) {
  const a = toFiniteNumber(left);
  const b = toFiniteNumber(right);
  if (a === null || b === null) {
    return null;
  }
  return Math.abs(a - b) / Math.max(Math.abs(b), 1);
}

function hasMaterialNumberChange(before, after, absThreshold = 0.01, ratioThreshold = 0.005) {
  const left = toFiniteNumber(before);
  const right = toFiniteNumber(after);
  if (left === null && right === null) {
    return false;
  }
  if (left === null || right === null) {
    return true;
  }
  const diff = Math.abs(left - right);
  if (diff <= absThreshold) {
    return false;
  }
  return diff / Math.max(Math.abs(right), 1) > ratioThreshold;
}

function normalizeSnapshotMarketNameLabel(rawSymbol, marketName) {
  const normalized = normalizeSourceSymbol(rawSymbol);
  const text = String(marketName || "")
    .replace(/[+\-]?\d[\d,]*(?:\.\d+)?/g, "")
    .replace(/\s+/g, "")
    .trim();

  if (text.includes("港")) {
    return "港股通";
  }
  if (text.includes("深")) {
    return "深圳A股";
  }
  if (text.includes("上")) {
    return "上海A股";
  }

  if (!normalized) {
    return text;
  }

  if (normalized.market === "HK") {
    return "港股通";
  }
  if (normalized.market === "CN") {
    return /^[569]/.test(normalized.symbol) ? "上海A股" : "深圳A股";
  }

  return text;
}

function getSnapshotHoldingQty(row) {
  const candidates = [row?.holdingQty, row?.balanceQty, row?.availableQty, row?.changeQty]
    .map((item) => toPositiveNumber(item))
    .filter((item) => item !== null);

  if (candidates.length === 0) {
    return 0;
  }

  return Math.max(...candidates);
}

function buildKnownFixedSnapshotRows(snapshot) {
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];

  if (snapshot?.postId !== "xq:349905429") {
    return rows;
  }

  const hkShenhuaMatcher = (row) => {
    const symbol = String(row?.symbol || "").trim();
    const name = String(row?.name || "").trim();
    return name === "中国神华" && (symbol === "01068" || symbol === "01088");
  };

  const firstMatchIndex = rows.findIndex(hkShenhuaMatcher);
  if (firstMatchIndex < 0) {
    return rows;
  }

  const normalizedRows = rows.filter((row) => !hkShenhuaMatcher(row));
  normalizedRows.splice(firstMatchIndex, 0, {
    actionLabel: "持仓快照",
    action: "HOLD",
    symbol: "01088",
    name: "中国神华",
    changeQty: 20000,
    latestCost: null,
    holdingQty: 20000,
    balanceQty: 20000,
    availableQty: 20000,
    referenceCost: null,
    latestPrice: 32.0285,
    referenceHoldingCost: null,
    marketValue: 640570,
    floatingPnl: null,
    pnlPct: null,
    marketName: "港股通"
  });

  return normalizedRows;
}

function shouldDropSanitizedSnapshotRow(row) {
  if (!row || typeof row !== "object") {
    return false;
  }

  return String(row.action || "HOLD").toUpperCase() === "HOLD" && getSnapshotHoldingQty(row) <= 0;
}

function sanitizeMasterSnapshotRow(row) {
  if (!row || typeof row !== "object") {
    return row;
  }

  const action = String(row.action || "HOLD").toUpperCase();
  const output = {
    ...row,
    name: normalizeSecurityName(row.symbol, row.name, undefined, {
      nameSource: row.nameSource
    }),
    marketName: normalizeSnapshotMarketNameLabel(row.symbol, row.marketName)
  };

  if (action !== "HOLD") {
    const normalizedQty = toFiniteNumber(row.changeQty);
    const normalizedCost = toPositiveNumber(row.latestCost ?? row.referenceCost ?? row.latestPrice);

    if (normalizedQty !== null) {
      output.changeQty = Math.abs(normalizedQty);
    }
    if (normalizedCost !== null) {
      output.latestCost = roundNumeric(normalizedCost, 6);
      output.referenceCost = roundNumeric(toFiniteNumber(row.referenceCost) ?? normalizedCost, 6);
    }

    return output;
  }

  const holdingQty = getSnapshotHoldingQty(row);
  let referenceCost = toFiniteNumber(row.referenceCost ?? row.latestCost);
  let latestPrice = toPositiveNumber(row.latestPrice);
  let marketValue = toPositiveNumber(row.marketValue);
  let referenceHoldingCost = toFiniteNumber(row.referenceHoldingCost);
  let floatingPnl = toFiniteNumber(row.floatingPnl);
  let pnlPct = toFiniteNumber(row.pnlPct);

  if (holdingQty > 0) {
    output.changeQty = holdingQty;
    output.holdingQty = holdingQty;
    output.balanceQty = holdingQty;
    output.availableQty = holdingQty;
  }

  if (holdingQty > 0 && referenceCost !== null) {
    const derivedHoldingCost = holdingQty * referenceCost;
    const gap = numericGap(referenceHoldingCost, derivedHoldingCost);
    if (
      referenceHoldingCost === null ||
      Math.abs(referenceHoldingCost) < 1 ||
      (gap !== null && gap > 0.12 && Math.sign(referenceHoldingCost || 0) === Math.sign(derivedHoldingCost || 0))
    ) {
      referenceHoldingCost = derivedHoldingCost;
    }
  }

  if (holdingQty > 0 && marketValue !== null) {
    const derivedLatestPrice = marketValue / holdingQty;
    if (latestPrice === null) {
      latestPrice = derivedLatestPrice;
    } else {
      const gap = numericGap(latestPrice, derivedLatestPrice);
      if (gap !== null && gap > 0.12) {
        latestPrice = derivedLatestPrice;
      }
    }
  } else if (holdingQty > 0 && latestPrice !== null) {
    marketValue = holdingQty * latestPrice;
  }

  if (referenceHoldingCost !== null && referenceHoldingCost > 0 && marketValue !== null) {
    const derivedFloatingPnl = marketValue - referenceHoldingCost;
    if (
      floatingPnl === null ||
      Math.abs(floatingPnl - derivedFloatingPnl) > Math.max(100, Math.abs(derivedFloatingPnl) * 0.08)
    ) {
      floatingPnl = derivedFloatingPnl;
    }

    const derivedPnlPct = derivedFloatingPnl / referenceHoldingCost * 100;
    if (
      pnlPct === null ||
      Math.abs(pnlPct) > 1000 ||
      Math.abs(pnlPct - derivedPnlPct) > Math.max(3, Math.abs(derivedPnlPct) * 0.12)
    ) {
      pnlPct = derivedPnlPct;
    }
  }

  if (referenceHoldingCost !== null && referenceHoldingCost <= 0) {
    floatingPnl = null;
    if (pnlPct !== null && Math.abs(pnlPct) > 500) {
      pnlPct = null;
    }
  }

  if (referenceCost !== null) {
    output.referenceCost = roundNumeric(referenceCost, 6);
    output.latestCost = roundNumeric(referenceCost, 6);
  }
  if (latestPrice !== null) {
    output.latestPrice = roundNumeric(latestPrice, 6);
  }
  if (marketValue !== null) {
    output.marketValue = roundNumeric(marketValue, 2);
  }
  if (referenceHoldingCost !== null) {
    output.referenceHoldingCost = roundNumeric(referenceHoldingCost, 2);
  }
  output.floatingPnl = floatingPnl === null ? null : roundNumeric(floatingPnl, 2);
  output.pnlPct = pnlPct === null ? null : roundNumeric(pnlPct, 3);

  return output;
}

function buildSnapshotRowFixes(rawRow, sanitizedRow) {
  const fixes = [];
  if (String(rawRow?.marketName || "").trim() !== String(sanitizedRow?.marketName || "").trim()) {
    fixes.push("市场字段已归一化");
  }
  if (hasMaterialNumberChange(rawRow?.latestPrice, sanitizedRow?.latestPrice)) {
    fixes.push("最新价已按市值重算");
  }
  if (hasMaterialNumberChange(rawRow?.referenceHoldingCost, sanitizedRow?.referenceHoldingCost, 1, 0.01)) {
    fixes.push("持仓成本已重算");
  }
  if (hasMaterialNumberChange(rawRow?.floatingPnl, sanitizedRow?.floatingPnl, 1, 0.01)) {
    fixes.push("浮动盈亏已重算");
  }
  if (hasMaterialNumberChange(rawRow?.pnlPct, sanitizedRow?.pnlPct, 0.1, 0.01)) {
    fixes.push("盈亏比例已重算");
  }
  return fixes;
}

function buildSnapshotRowIssues(row) {
  const issues = [];
  const holdingQty = getSnapshotHoldingQty(row);
  const referenceCost = toFiniteNumber(row?.referenceCost ?? row?.latestCost);
  const latestPrice = toPositiveNumber(row?.latestPrice);
  const marketValue = toPositiveNumber(row?.marketValue);
  const referenceHoldingCost = toFiniteNumber(row?.referenceHoldingCost);
  const floatingPnl = toFiniteNumber(row?.floatingPnl);
  const pnlPct = toFiniteNumber(row?.pnlPct);
  const marketName = String(row?.marketName || "").trim();

  if (holdingQty <= 0) {
    issues.push("持仓股数缺失");
  }
  if (marketValue === null) {
    issues.push("市值缺失");
  }
  if (!marketName) {
    issues.push("市场字段缺失");
  }
  if (referenceCost !== null && referenceCost <= 0) {
    issues.push("成本价异常");
  }
  if (referenceHoldingCost !== null && referenceHoldingCost <= 0) {
    issues.push("持仓成本异常");
  }
  if (holdingQty > 0 && latestPrice !== null && referenceCost !== null && referenceCost > latestPrice * 20) {
    issues.push("成本价疑似识别异常");
  }

  if (holdingQty > 0 && latestPrice !== null && marketValue !== null) {
    const priceGap = numericGap(marketValue, holdingQty * latestPrice);
    if (priceGap !== null && priceGap > 0.12) {
      issues.push("最新价与市值不一致");
    }
  }

  if (holdingQty > 0 && referenceCost !== null && referenceHoldingCost !== null && referenceHoldingCost > 0) {
    const costGap = numericGap(referenceHoldingCost, holdingQty * referenceCost);
    if (costGap !== null && costGap > 0.12) {
      issues.push("持仓成本与成本价不一致");
    }
  }

  if (referenceHoldingCost !== null && referenceHoldingCost > 0 && marketValue !== null) {
    const derivedFloatingPnl = marketValue - referenceHoldingCost;
    if (
      floatingPnl !== null &&
      Math.abs(floatingPnl - derivedFloatingPnl) > Math.max(100, Math.abs(derivedFloatingPnl) * 0.08)
    ) {
      issues.push("浮动盈亏仍需复核");
    }

    const derivedPnlPct = derivedFloatingPnl / referenceHoldingCost * 100;
    if (pnlPct !== null && Math.abs(pnlPct - derivedPnlPct) > Math.max(3, Math.abs(derivedPnlPct) * 0.12)) {
      issues.push("盈亏比例仍需复核");
    }
  }

  return issues;
}

function sanitizeMasterSnapshotRecord(snapshot, options = {}) {
  if (!snapshot || typeof snapshot !== "object") {
    return null;
  }

  const includeDiagnostics = Boolean(options.includeDiagnostics);
  const rows = buildKnownFixedSnapshotRows(snapshot);
  let changedRowCount = 0;
  let issueRowCount = 0;

  const sanitizedRows = rows.flatMap((row) => {
    const sanitizedRow = sanitizeMasterSnapshotRow(row);

    if (shouldDropSanitizedSnapshotRow(sanitizedRow)) {
      changedRowCount += 1;
      return [];
    }

    const fixes = buildSnapshotRowFixes(row, sanitizedRow);
    const issues = buildSnapshotRowIssues(sanitizedRow);

    if (fixes.length > 0) {
      changedRowCount += 1;
    }
    if (issues.length > 0) {
      issueRowCount += 1;
    }

    if (!includeDiagnostics) {
      return sanitizedRow;
    }

    return {
      ...sanitizedRow,
      diagnostics:
        fixes.length > 0 || issues.length > 0
          ? {
              fixes,
              issues,
              level: issues.length > 0 ? "warn" : "fix"
            }
          : null
    };
  });

  return {
    ...snapshot,
    rows: sanitizedRows,
    ...extractSnapshotPostMetrics(snapshot),
    anomalySummary: {
      rowCount: sanitizedRows.length,
      changedRowCount,
      issueRowCount
    }
  };
}

function sanitizeSnapshotCollection(snapshots, options = {}) {
  return (Array.isArray(snapshots) ? snapshots : []).map((snapshot) => sanitizeMasterSnapshotRecord(snapshot, options));
}

function stripSnapshotDiagnostics(snapshot) {
  if (!snapshot || typeof snapshot !== "object") {
    return snapshot;
  }

  return {
    ...snapshot,
    rows: Array.isArray(snapshot.rows)
      ? snapshot.rows.map((row) => {
          if (!row || typeof row !== "object") {
            return row;
          }
          const { diagnostics, ...rest } = row;
          return rest;
        })
      : []
  };
}

function buildAnomalyReportFromSnapshots(snapshotsWithDiagnostics) {
  const snapshots = (Array.isArray(snapshotsWithDiagnostics) ? snapshotsWithDiagnostics : [])
    .map((snapshot) => ({
      ...snapshot,
      rows: (snapshot.rows || []).filter(
        (row) => row.diagnostics && (row.diagnostics.fixes.length > 0 || row.diagnostics.issues.length > 0)
      )
    }))
    .filter((snapshot) => snapshot.rows.length > 0);

  const summary = snapshots.reduce(
    (acc, snapshot) => {
      acc.snapshotCount += 1;
      acc.rowCount += snapshot.rows.length;
      acc.changedRowCount += Number(snapshot.anomalySummary?.changedRowCount) || 0;
      acc.issueRowCount += Number(snapshot.anomalySummary?.issueRowCount) || 0;
      return acc;
    },
    {
      snapshotCount: 0,
      rowCount: 0,
      changedRowCount: 0,
      issueRowCount: 0
    }
  );

  return {
    summary,
    snapshots
  };
}

function mergeSnapshotMetricFields(snapshot) {
  if (!snapshot || typeof snapshot !== "object") {
    return snapshot;
  }

  const manualMetrics = normalizeManualMetrics(snapshot.manualMetrics || null);
  const metrics = extractSnapshotPostMetrics(buildMetricsSourceSnapshot(snapshot, manualMetrics));
  return {
    ...snapshot,
    manualMetrics,
    cumulativeNetValue: metrics.cumulativeNetValue,
    netIndex: metrics.netIndex,
    yearStartNetIndex: metrics.yearStartNetIndex
  };
}

function mergeMonthlyUpdateMetricFields(update, metrics) {
  if (!update || typeof update !== "object") {
    return update;
  }

  const netValue = toFiniteNumber(metrics?.cumulativeNetValue ?? update.netValue);
  const netIndex = toFiniteNumber(metrics?.netIndex ?? update.netIndex);
  const yearStartNetIndex = toFiniteNumber(metrics?.yearStartNetIndex ?? update.yearStartNetIndex);

  return {
    ...update,
    netValue: netValue === null ? null : roundNumeric(netValue, 2),
    netIndex: netIndex === null ? null : roundNumeric(netIndex, 4),
    yearStartNetIndex: yearStartNetIndex === null ? null : roundNumeric(yearStartNetIndex, 4)
  };
}

function buildMetricsSourceSnapshot(snapshot, manualMetrics = null) {
  if (!snapshot || typeof snapshot !== "object") {
    return {
      manualMetrics: normalizeManualMetrics(manualMetrics),
      cumulativeNetValue: null,
      netIndex: null,
      yearStartNetIndex: null
    };
  }

  return {
    ...snapshot,
    manualMetrics: normalizeManualMetrics(manualMetrics),
    cumulativeNetValue: null,
    netIndex: null,
    yearStartNetIndex: null
  };
}

function parseNullableMetricNumber(rawValue, fieldLabel) {
  if (rawValue === null || typeof rawValue === "undefined") {
    return undefined;
  }

  const text = String(rawValue).trim();
  if (!text) {
    return null;
  }

  const number = Number(text);
  if (!Number.isFinite(number) || number <= 0) {
    throw createHttpError(400, `${fieldLabel} 必须大于 0`);
  }

  return number;
}

function normalizeManualMetrics(manualMetrics) {
  if (!manualMetrics || typeof manualMetrics !== "object") {
    return null;
  }

  const normalized = {
    cumulativeNetValue: toPositiveNumber(manualMetrics.cumulativeNetValue),
    netIndex: toPositiveNumber(manualMetrics.netIndex),
    yearStartNetIndex: toPositiveNumber(manualMetrics.yearStartNetIndex)
  };

  if (
    normalized.cumulativeNetValue === null &&
    normalized.netIndex === null &&
    normalized.yearStartNetIndex === null
  ) {
    return null;
  }

  return normalized;
}

function extractManualOnlyMetrics(manualMetrics) {
  return extractSnapshotPostMetrics(buildMetricsSourceSnapshot(null, normalizeManualMetrics(manualMetrics)));
}

function normalizePostId(value) {
  return String(value || "").trim();
}

function findSnapshotIndexByMonthlyUpdate(snapshots, update) {
  const list = Array.isArray(snapshots) ? snapshots : [];
  const postId = normalizePostId(update?.postId);
  if (postId) {
    const directIndex = list.findIndex((item) => normalizePostId(item.postId) === postId);
    if (directIndex >= 0) {
      return directIndex;
    }
  }

  const source = String(update?.source || "").trim().toLowerCase();
  const month = String(update?.month || toMonthKey(update?.postedAt)).trim();
  if (!source || !month) {
    return -1;
  }

  return list.findIndex(
    (item) =>
      String(item?.source || "").trim().toLowerCase() === source &&
      String(toMonthKey(item?.postedAt) || "").trim() === month
  );
}

async function backfillStoredPostMetrics() {
  await mutateStore((draft) => {
    const snapshotMetricsByMonthlyId = new Map();
    const snapshotMetricsByMonth = new Map();
    const snapshotMetaByMonthlyId = new Map();
    const snapshotMetaByMonth = new Map();

    draft.masterSnapshots = (draft.masterSnapshots || []).map((snapshot) => {
      const nextSnapshot = mergeSnapshotMetricFields(snapshot);
      const metrics = extractSnapshotPostMetrics(nextSnapshot);
      const month = toMonthKey(nextSnapshot.postedAt);
      const monthlyId = `${nextSnapshot.source}:${nextSnapshot.postId}`;
      const meta = {
        postId: nextSnapshot.postId || null,
        title: nextSnapshot.title || "",
        link: nextSnapshot.link || ""
      };

      snapshotMetricsByMonthlyId.set(monthlyId, metrics);
      snapshotMetricsByMonth.set(`${nextSnapshot.source}:${month}`, metrics);
      snapshotMetaByMonthlyId.set(monthlyId, meta);
      snapshotMetaByMonth.set(`${nextSnapshot.source}:${month}`, meta);
      return nextSnapshot;
    });

    draft.monthlyUpdates = (draft.monthlyUpdates || []).map((update) => {
      const manualMetrics = normalizeManualMetrics(update.manualMetrics || null);
      const meta =
        snapshotMetaByMonthlyId.get(String(update.id || "").trim()) ||
        snapshotMetaByMonth.get(`${update.source}:${update.month}`) ||
        null;
      const snapshotMetrics =
        snapshotMetricsByMonthlyId.get(String(update.id || "").trim()) ||
        snapshotMetricsByMonth.get(`${update.source}:${update.month}`) ||
        null;
      const manualOnlyMetrics = extractManualOnlyMetrics(manualMetrics);
      const metrics = {
        cumulativeNetValue: snapshotMetrics?.cumulativeNetValue ?? manualOnlyMetrics.cumulativeNetValue,
        netIndex: snapshotMetrics?.netIndex ?? manualOnlyMetrics.netIndex,
        yearStartNetIndex: snapshotMetrics?.yearStartNetIndex ?? manualOnlyMetrics.yearStartNetIndex
      };
      return {
        ...mergeMonthlyUpdateMetricFields(
          {
            ...update,
            netValue: null,
            netIndex: null,
            yearStartNetIndex: null,
            manualMetrics
          },
          metrics
        ),
        postId: String(update.postId || meta?.postId || "").trim() || null,
        title: String(update.title || meta?.title || "").trim(),
        link: String(update.link || meta?.link || "").trim()
      };
    });

    const autoTracking = ensureAutoTrackingState(draft);
    if (autoTracking.latestSnapshot) {
      autoTracking.latestSnapshot = mergeSnapshotMetricFields(autoTracking.latestSnapshot);
    } else {
      autoTracking.latestSnapshot = draft.masterSnapshots[0] || null;
    }
  });
}

function getAutoTrackingPublic(autoTrackingInput, options = {}) {
  const autoTracking = autoTrackingInput || {};
  const config = mergeAutoTrackingConfig(autoTracking.config || {});
  const includeLatestSnapshot = options.includeLatestSnapshot !== false;
  const latestSnapshot = Object.prototype.hasOwnProperty.call(options, "latestSnapshot")
    ? options.latestSnapshot
    : autoTracking.latestSnapshot
      ? sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot)
      : null;

  const payload = {
    config: {
      enabled: Boolean(config.enabled),
      intervalMinutes: config.intervalMinutes,
      smartScheduleEnabled: Boolean(config.smartScheduleEnabled),
      monthEndWindowDays: config.monthEndWindowDays,
      offWindowIntervalHours: config.offWindowIntervalHours,
      skipStartupOutsideWindow: Boolean(config.skipStartupOutsideWindow),
      cookieKeepAliveEnabled: Boolean(config.cookieKeepAliveEnabled),
      cookieKeepAliveIntervalHours: config.cookieKeepAliveIntervalHours,
      scheduleTimeZone: AUTO_TRACKING_TIME_ZONE,
      maxPostsPerSource: config.maxPostsPerSource,
      ocrEnabled: Boolean(config.ocrEnabled),
      ocrProvider: config.ocrProvider,
      ocrMaxImagesPerPost: config.ocrMaxImagesPerPost,
      pinnedPostUrls: config.pinnedPostUrls,
      xueqiuTitleRegex: config.xueqiuTitleRegex,
      backfillMaxPages: config.backfillMaxPages,
      backfillPageSize: config.backfillPageSize,
      keywords: config.keywords,
      hasQwenApiKey: Boolean(config.qwenApiKey) || Boolean(process.env.DASHSCOPE_API_KEY),
      hasXueqiuCookie: Boolean(config.xueqiuCookie) && !isSampleCookie(config.xueqiuCookie),
      hasWeiboCookie: Boolean(config.weiboCookie) && !isSampleCookie(config.weiboCookie)
    },
    runtime: autoTracking.runtime || {},
    recentLogs: Array.isArray(autoTracking.logs) ? autoTracking.logs.slice(0, 30) : []
  };

  if (includeLatestSnapshot) {
    payload.latestSnapshot = latestSnapshot;
  }

  return payload;
}

function appendAutoTrackingLogs(autoTracking, logs) {
  if (!Array.isArray(logs) || logs.length === 0) {
    return;
  }

  autoTracking.logs = [...logs, ...(autoTracking.logs || [])].slice(0, 200);
}

function buildNameValidationSourceStats(namesBySymbol, sourcesBySymbol) {
  const stats = {
    total: 0,
    xueqiu: 0,
    tencent: 0,
    other: 0
  };

  for (const apiSymbol of Object.keys(namesBySymbol || {})) {
    const name = String(namesBySymbol?.[apiSymbol] || "").trim();
    if (!name) {
      continue;
    }

    stats.total += 1;
    const source = String(sourcesBySymbol?.[apiSymbol] || "").trim().toLowerCase();
    if (source === "xueqiu") {
      stats.xueqiu += 1;
    } else if (source === "tencent") {
      stats.tencent += 1;
    } else {
      stats.other += 1;
    }
  }

  return stats;
}

function buildNameValidationLog(stats, label = "名称校验") {
  if (!stats || stats.total <= 0) {
    return null;
  }

  const parts = [`雪球 ${stats.xueqiu}`];
  if (stats.tencent > 0) {
    parts.push(`腾讯备份 ${stats.tencent}`);
  }
  if (stats.other > 0) {
    parts.push(`其他 ${stats.other}`);
  }

  return {
    id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
    createdAt: new Date().toISOString(),
    level: "info",
    message: `${label}完成：共 ${stats.total} 只，${parts.join("，")}`,
    meta: stats
  };
}

function buildPositionQuantityIndex(trades, quotes) {
  const quantityBySymbol = new Map();
  const { positions } = buildPortfolio(trades, quotes);

  for (const position of positions) {
    const apiSymbol = String(position?.apiSymbol || "").trim();
    const quantity = Number(position?.quantity) || 0;
    if (!apiSymbol || quantity <= 0) {
      continue;
    }
    quantityBySymbol.set(apiSymbol, quantity);
  }

  return quantityBySymbol;
}

function getTrackedQuantity(quantityBySymbol, apiSymbol) {
  return Number(quantityBySymbol.get(apiSymbol)) || 0;
}

function isAutoImportedTradeForPost(trade, postId) {
  const normalizedPostId = normalizePostId(postId);
  if (!normalizedPostId) {
    return false;
  }

  const note = String(trade?.note || "").trim();
  return note.startsWith("auto_sync:") && note.includes(`:${normalizedPostId}:`);
}

function purgeAutoImportedSnapshotData(store, importedTradeKeys, postId) {
  const normalizedPostId = normalizePostId(postId);
  if (!normalizedPostId) {
    return 0;
  }

  const currentTrades = Array.isArray(store?.trades) ? store.trades : [];
  const nextTrades = currentTrades.filter((trade) => !isAutoImportedTradeForPost(trade, normalizedPostId));
  store.trades = nextTrades;

  for (const key of Array.from(importedTradeKeys)) {
    if (String(key || "").startsWith(`${normalizedPostId}|`)) {
      importedTradeKeys.delete(key);
    }
  }

  return currentTrades.length - nextTrades.length;
}

function applyTrackedTrade(quantityBySymbol, trade) {
  const apiSymbol = String(trade?.apiSymbol || "").trim();
  if (!apiSymbol) {
    return;
  }

  const quantity = Math.max(0, Number(trade?.quantity) || 0);
  if (quantity <= 0) {
    return;
  }

  const current = getTrackedQuantity(quantityBySymbol, apiSymbol);
  const type = String(trade?.type || "").trim().toUpperCase();

  if (type === "BUY") {
    quantityBySymbol.set(apiSymbol, current + quantity);
    return;
  }

  if (type === "SELL") {
    const next = Math.max(0, current - quantity);
    if (next > 0) {
      quantityBySymbol.set(apiSymbol, next);
    } else {
      quantityBySymbol.delete(apiSymbol);
    }
  }
}

async function scheduleAutoTracking() {
  if (autoTrackingTimer) {
    clearTimeout(autoTrackingTimer);
    autoTrackingTimer = null;
  }

  const store = await readStore();
  const autoTracking = ensureAutoTrackingState(store);
  const config = mergeAutoTrackingConfig(autoTracking.config);
  const schedule = buildAutoTrackingSchedule(config, new Date());

  if (!config.enabled) {
    await mutateStore((draft) => {
      const state = ensureAutoTrackingState(draft);
      state.runtime.nextRunAt = null;
      state.runtime.scheduleMode = schedule.mode;
      state.runtime.scheduleHint = schedule.scheduleHint;
    });
    return;
  }

  autoTrackingTimer = setTimeout(() => {
    autoTrackingTimer = null;
    runAutoTrackingWithJob("timer", {}, {
      label: "定时抓取",
      startMessage: "定时任务正在抓取最新帖子",
      successMessage: "定时抓取完成"
    }).catch((error) => {
      console.error("Auto tracking timer error:", error.message);
    });
  }, schedule.delayMs);

  if (typeof autoTrackingTimer?.unref === "function") {
    autoTrackingTimer.unref();
  }

  await mutateStore((draft) => {
    const state = ensureAutoTrackingState(draft);
    state.runtime.nextRunAt = schedule.nextRunAt ? schedule.nextRunAt.toISOString() : null;
    state.runtime.scheduleMode = schedule.mode;
    state.runtime.scheduleHint = schedule.scheduleHint;
  });
}

async function scheduleCookieKeepAlive() {
  if (cookieKeepAliveTimer) {
    clearTimeout(cookieKeepAliveTimer);
    cookieKeepAliveTimer = null;
  }

  const store = await readStore();
  const autoTracking = ensureAutoTrackingState(store);
  const config = mergeAutoTrackingConfig(autoTracking.config);
  const schedule = buildCookieKeepAliveSchedule(config, autoTracking.runtime, new Date());

  if (!config.cookieKeepAliveEnabled) {
    await mutateStore((draft) => {
      const state = ensureAutoTrackingState(draft);
      state.runtime.nextCookieKeepAliveAt = null;
    });
    return;
  }

  cookieKeepAliveTimer = setTimeout(() => {
    cookieKeepAliveTimer = null;
    runCookieKeepAliveJob("timer").catch((error) => {
      console.error("Cookie keep-alive timer error:", error.message);
    });
  }, schedule.delayMs);

  if (typeof cookieKeepAliveTimer?.unref === "function") {
    cookieKeepAliveTimer.unref();
  }

  await mutateStore((draft) => {
    const state = ensureAutoTrackingState(draft);
    state.runtime.nextCookieKeepAliveAt = schedule.nextRunAt ? schedule.nextRunAt.toISOString() : null;
  });
}

async function runAutoTrackingJob(trigger = "manual", collectOptions = {}) {
  if (autoTrackingRunning) {
    return {
      ok: false,
      skipped: true,
      reason: "任务正在执行中"
    };
  }

  autoTrackingRunning = true;
  const startedAt = new Date().toISOString();

  try {
    const before = await readStore();
    const autoTrackingBefore = ensureAutoTrackingState(before);
    const config = mergeAutoTrackingConfig(autoTrackingBefore.config);
    const forceRefresh = Boolean(collectOptions.forceRefresh);
    const selectedPostIds = new Set(normalizePostIds(collectOptions.targetPostIds));

    if (!config.enabled && trigger === "timer") {
      return {
        ok: true,
        skipped: true,
        reason: "自动同步已关闭"
      };
    }

    const syncResult = await collectSuperLudinggongSnapshots(
      config,
      forceRefresh ? [] : autoTrackingBefore.processedPostIds || [],
      collectOptions
    );

    try {
      const apiSymbols = collectSnapshotApiSymbols(syncResult.snapshots);
      const securityNames = await lookupSecurityNames(apiSymbols, {
        xueqiuCookie: config.xueqiuCookie
      });
      const nameStats = buildNameValidationSourceStats(securityNames.namesBySymbol, securityNames.sourcesBySymbol);
      applyCollectedSecurityNameValidation(
        syncResult.snapshots,
        securityNames.namesBySymbol,
        securityNames.sourcesBySymbol
      );
      const nameLog = buildNameValidationLog(nameStats, "名称校验");
      if (nameLog) {
        syncResult.logs = [nameLog, ...(syncResult.logs || [])];
      }
    } catch (_error) {
      // Best-effort name validation should not block snapshot import.
    }

    let importedSnapshots = 0;
    let importedTrades = 0;
    let skippedSnapshots = 0;

    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      const processedPostIds = new Set(autoTracking.processedPostIds || []);
      const importedTradeKeys = new Set(autoTracking.importedTradeKeys || []);
      let quantityBySymbol = buildPositionQuantityIndex(draft.trades, draft.quotes);

      appendAutoTrackingLogs(autoTracking, syncResult.logs);

      for (const snapshot of syncResult.snapshots) {
        const sanitizedSnapshot = sanitizeMasterSnapshotRecord(snapshot);
        const shouldRefreshSnapshot = forceRefresh && selectedPostIds.has(snapshot.postId);
        if (processedPostIds.has(sanitizedSnapshot.postId) && !shouldRefreshSnapshot) {
          skippedSnapshots += 1;
          continue;
        }

        const existingSnapshotRecord = (draft.masterSnapshots || []).find((item) => item.postId === sanitizedSnapshot.postId) || null;
        const month = toMonthKey(sanitizedSnapshot.postedAt);
        const monthKey = `${sanitizedSnapshot.source}:${sanitizedSnapshot.postId}`;
        const existingMonthIndex = draft.monthlyUpdates.findIndex((item) => item.id === monthKey);
        const existingMonthRecord = existingMonthIndex >= 0 ? draft.monthlyUpdates[existingMonthIndex] : null;
        const preservedManualMetrics = normalizeManualMetrics(
          existingSnapshotRecord?.manualMetrics || existingMonthRecord?.manualMetrics || null
        );
        const snapshotMetrics = extractSnapshotPostMetrics(
          buildMetricsSourceSnapshot(sanitizedSnapshot, preservedManualMetrics)
        );

        if (shouldRefreshSnapshot) {
          const removedTradeCount = purgeAutoImportedSnapshotData(draft, importedTradeKeys, sanitizedSnapshot.postId);
          if (removedTradeCount > 0) {
            quantityBySymbol = buildPositionQuantityIndex(draft.trades, draft.quotes);
          }
        }

        let importedTradesInSnapshot = 0;

        for (const row of sanitizedSnapshot.rows) {
          if (!["BUY", "SELL"].includes(row.action)) {
            continue;
          }

          const normalized = normalizeSourceSymbol(row.symbol);
          if (!normalized) {
            continue;
          }

          const rawQty = Math.abs(Number(row.changeQty) || 0);
          const rawPrice = Math.abs(Number(row.latestCost) || 0);

          if (rawQty <= 0 || rawPrice <= 0) {
            continue;
          }

          let quantity = rawQty;
          const apiSymbol = toApiSymbol(normalized.symbol, normalized.market);
          const dedupeKey = `${sanitizedSnapshot.postId}|${apiSymbol}|${row.action}|${rawQty}|${rawPrice}`;
          if (importedTradeKeys.has(dedupeKey)) {
            continue;
          }

          if (row.action === "SELL") {
            const available = getTrackedQuantity(quantityBySymbol, apiSymbol);
            if (available <= 0) {
              continue;
            }
            quantity = Math.min(quantity, available);
          }

          if (quantity <= 0) {
            continue;
          }

          const trade = parseTradeInput({
            symbol: normalized.symbol,
            market: normalized.market,
            type: row.action,
            quantity,
            price: rawPrice,
            fee: 0,
            tradeDate: sanitizedSnapshot.postedAt,
            name: row.name || "",
            nameSource: row.nameSource || "",
            note: `auto_sync:${sanitizedSnapshot.source}:${sanitizedSnapshot.postId}:${row.actionLabel || row.action}`
          });

          draft.trades.push(trade);
          applyTrackedTrade(quantityBySymbol, trade);
          importedTradeKeys.add(dedupeKey);
          importedTrades += 1;
          importedTradesInSnapshot += 1;
        }

        const notePrefix = shouldRefreshSnapshot || existingMonthIndex >= 0 ? "自动重抓" : "自动抓取";
        const nextMonthRecord = {
          ...(existingMonthRecord || {}),
          id: monthKey,
          month,
          source: sanitizedSnapshot.source,
          postId: sanitizedSnapshot.postId,
          title: sanitizedSnapshot.title || "",
          link: sanitizedSnapshot.link || "",
          postedAt: sanitizedSnapshot.postedAt,
          note: `${notePrefix}: ${sanitizedSnapshot.rows.length} 行`,
          netValue: snapshotMetrics.cumulativeNetValue,
          netIndex: snapshotMetrics.netIndex,
          yearStartNetIndex: snapshotMetrics.yearStartNetIndex,
          manualMetrics: preservedManualMetrics,
          createdAt:
            existingMonthRecord ? existingMonthRecord.createdAt : new Date().toISOString(),
          updatedAt: new Date().toISOString()
        };

        if (existingMonthIndex >= 0) {
          draft.monthlyUpdates[existingMonthIndex] = nextMonthRecord;
        } else {
          draft.monthlyUpdates.push(nextMonthRecord);
        }

        const latestSnapshotRecord = {
          id: randomUUID(),
          postId: sanitizedSnapshot.postId,
          source: sanitizedSnapshot.source,
          postedAt: sanitizedSnapshot.postedAt,
          link: sanitizedSnapshot.link,
          title: sanitizedSnapshot.title || "",
          rows: sanitizedSnapshot.rows,
          rawText: sanitizedSnapshot.rawText,
          ocrText: sanitizedSnapshot.ocrText,
          cumulativeNetValue: snapshotMetrics.cumulativeNetValue,
          netIndex: snapshotMetrics.netIndex,
          yearStartNetIndex: snapshotMetrics.yearStartNetIndex,
          manualMetrics: preservedManualMetrics,
          images: sanitizedSnapshot.images,
          importedTrades: importedTradesInSnapshot,
          createdAt: new Date().toISOString()
        };

        draft.masterSnapshots = [
          latestSnapshotRecord,
          ...(draft.masterSnapshots || []).filter((item) => item.postId !== sanitizedSnapshot.postId)
        ].slice(0, 200);
        autoTracking.latestSnapshot = latestSnapshotRecord;

        processedPostIds.add(sanitizedSnapshot.postId);
        importedSnapshots += 1;
      }

      autoTracking.processedPostIds = [...processedPostIds].slice(-1500);
      autoTracking.importedTradeKeys = [...importedTradeKeys].slice(-6000);

      draft.masterSnapshots = sortByRecentDate(draft.masterSnapshots || [], "postedAt").slice(0, 200);
      autoTracking.latestSnapshot = draft.masterSnapshots[0] || null;

      autoTracking.runtime.lastRunAt = startedAt;
      autoTracking.runtime.lastError = null;
      autoTracking.runtime.lastSuccessAt = new Date().toISOString();
      autoTracking.runtime.totalImportedSnapshots =
        (Number(autoTracking.runtime.totalImportedSnapshots) || 0) + importedSnapshots;
      autoTracking.runtime.totalImportedTrades =
        (Number(autoTracking.runtime.totalImportedTrades) || 0) + importedTrades;
    });

    invalidateCatalogCache();

    return {
      ok: true,
      mode: collectOptions.mode || "normal",
      importedSnapshots,
      importedTrades,
      skippedSnapshots,
      logs: syncResult.logs
    };
  } catch (error) {
    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      autoTracking.runtime.lastRunAt = startedAt;
      autoTracking.runtime.lastError = error.message;
      appendAutoTrackingLogs(autoTracking, [
        {
          id: `${Date.now()}-fatal`,
          createdAt: new Date().toISOString(),
          level: "error",
          message: `自动同步失败: ${error.message}`
        }
      ]);
    });

    return {
      ok: false,
      error: error.message
    };
  } finally {
    autoTrackingRunning = false;
    try {
      await scheduleAutoTracking();
    } catch (scheduleError) {
      console.error("Auto tracking reschedule error:", scheduleError.message);
    }
  }
}

async function runAutoTrackingWithJob(trigger = "manual", collectOptions = {}, jobOptions = {}) {
  const job = createJob("auto_tracking_run", {
    label: jobOptions.label || "自动抓取"
  });

  try {
    startJob(job.jobId, {
      stage: "collect",
      progress: 10,
      message: jobOptions.startMessage || "正在抓取最新帖子"
    });

    const result = await runAutoTrackingJob(trigger, collectOptions);

    if (result.skipped) {
      skipJob(job.jobId, result.reason || "任务已跳过");
    } else if (result.ok) {
      finishJob(job.jobId, {
        summary: summarizeAutoTrackingResult(result),
        logs: result.logs,
        message: jobOptions.successMessage || "抓取完成"
      });
    } else {
      failJob(job.jobId, new Error(result.error || result.reason || "抓取失败"), {
        stage: "collect",
        summary: summarizeAutoTrackingResult(result)
      });
    }

    return {
      result,
      job: getJob(job.jobId)
    };
  } catch (error) {
    failJob(job.jobId, error, { stage: "collect" });
    throw error;
  }
}

async function runCookieKeepAliveJob(trigger = "manual") {
  if (cookieKeepAliveRunning) {
    return {
      ok: false,
      skipped: true,
      reason: "Cookie 保活任务正在执行中"
    };
  }

  cookieKeepAliveRunning = true;
  const startedAt = new Date().toISOString();

  try {
    const before = await readStore();
    const autoTrackingBefore = ensureAutoTrackingState(before);
    const config = mergeAutoTrackingConfig(autoTrackingBefore.config);

    if (!config.cookieKeepAliveEnabled) {
      return {
        ok: true,
        skipped: true,
        reason: "Cookie 保活已关闭"
      };
    }

    const result = await keepCookiesAlive(config);
    const completedAt = new Date().toISOString();
    const firstFailureMessage =
      result.results.find((item) => item && item.ok === false && !item.skipped)?.message || null;

    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      autoTracking.runtime.lastCookieKeepAliveAt = startedAt;
      if (result.successCount > 0) {
        autoTracking.runtime.lastCookieKeepAliveSuccessAt = completedAt;
      }
      autoTracking.runtime.lastCookieKeepAliveError = firstFailureMessage;
      appendAutoTrackingLogs(autoTracking, result.logs);
    });

    return result;
  } catch (error) {
    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      autoTracking.runtime.lastCookieKeepAliveAt = startedAt;
      autoTracking.runtime.lastCookieKeepAliveError = error.message;
      appendAutoTrackingLogs(autoTracking, [
        {
          id: `${Date.now()}-cookie-keepalive-fatal`,
          createdAt: new Date().toISOString(),
          level: "error",
          message: `Cookie 保活失败: ${error.message}`
        }
      ]);
    });

    return {
      ok: false,
      error: error.message
    };
  } finally {
    cookieKeepAliveRunning = false;
    try {
      await scheduleCookieKeepAlive();
    } catch (scheduleError) {
      console.error("Cookie keep-alive reschedule error:", scheduleError.message);
    }
  }
}

function sortByRecentDate(items, dateField = "createdAt") {
  return [...items].sort((a, b) => {
    const aTime = new Date(a[dateField] || a.createdAt || 0).getTime();
    const bTime = new Date(b[dateField] || b.createdAt || 0).getTime();
    return bTime - aTime;
  });
}

function normalizePostIds(input) {
  return normalizeRequestedPostIds(input);
}

function hasOwnPropertyValue(object, key) {
  return Boolean(object) && Object.prototype.hasOwnProperty.call(object, key);
}

function readPatchedSecretValue(payload, valueKey, clearKey, fallbackValue = "") {
  if (hasOwnPropertyValue(payload, clearKey) && Boolean(payload[clearKey])) {
    return "";
  }

  if (!hasOwnPropertyValue(payload, valueKey)) {
    return fallbackValue;
  }

  const nextValue = String(payload[valueKey] || "").trim();
  return nextValue || fallbackValue;
}

function buildMonthlyUpdatesPayload(store) {
  return {
    updates: sortByRecentDate(store.monthlyUpdates || [], "postedAt"),
    monthlyStatus: buildMonthlyStatus(store.monthlyUpdates || []),
    links: PROFILE_LINKS
  };
}

function buildMasterSnapshotSummary(snapshot) {
  if (!snapshot || typeof snapshot !== "object") {
    return null;
  }

  return {
    id: snapshot.id || null,
    postId: snapshot.postId || null,
    source: snapshot.source || "",
    postedAt: snapshot.postedAt || null,
    link: snapshot.link || "",
    title: snapshot.title || "",
    rowCount: Array.isArray(snapshot.rows) ? snapshot.rows.length : 0,
    importedTrades: Number(snapshot.importedTrades) || 0,
    createdAt: snapshot.createdAt || null,
    updatedAt: snapshot.updatedAt || null
  };
}

function buildAutoTrackingBootstrapPayload(store, options = {}) {
  const autoTracking = ensureAutoTrackingState(store);
  const limit = Math.max(1, Math.min(240, Number(options.limit) || 240));
  const snapshotSource = (store.masterSnapshots || []).slice(0, limit);
  const latestSnapshotSource = snapshotSource[0] || autoTracking.latestSnapshot || null;
  const latestSnapshot = sanitizeMasterSnapshotRecord(latestSnapshotSource);
  const snapshotHistory = snapshotSource.map((snapshot) => buildMasterSnapshotSummary(snapshot)).filter(Boolean);

  return {
    autoTracking: getAutoTrackingPublic(autoTracking, {
      includeLatestSnapshot: false
    }),
    latestSnapshot,
    snapshotHistory,
    monthlyUpdates: buildMonthlyUpdatesPayload(store)
  };
}

app.get("/api/jobs/overview", (_req, res) => {
  res.json(getJobOverview());
});

app.get("/api/jobs/:jobId", (req, res, next) => {
  try {
    const job = getJob(String(req.params.jobId || ""));
    if (!job) {
      throw createHttpError(404, "任务不存在");
    }
    res.json(job);
  } catch (error) {
    next(error);
  }
});

app.get("/api/health", async (_req, res, next) => {
  try {
    res.json({
      ok: true,
      now: new Date().toISOString(),
      version: APP_META.version,
      store: await getStoreHealthSummary(),
      jobs: getJobOverview()
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/app-meta", (_req, res) => {
  setNoStoreHeaders(res);
  res.json(APP_META);
});

app.get("/api/state", async (_req, res, next) => {
  try {
    const store = await readStore();
    res.json(buildState(store));
  } catch (error) {
    next(error);
  }
});

app.get("/api/trades", async (_req, res, next) => {
  try {
    const store = await readStore();
    const trades = sortByRecentDate(store.trades, "tradeDate");
    res.json({ trades });
  } catch (error) {
    next(error);
  }
});

app.post("/api/trades", async (req, res, next) => {
  try {
    const trade = parseTradeInput(req.body || {});

    await mutateStore((store) => {
      if (trade.type === "SELL") {
        const { positions } = buildPortfolio(store.trades, store.quotes);
        const current = positions.find((item) => item.apiSymbol === trade.apiSymbol);
        const available = current?.quantity || 0;

        if (trade.quantity > available) {
          throw createHttpError(
            400,
            `卖出数量超过当前持仓：可卖 ${available}，请求卖出 ${trade.quantity}`
          );
        }
      }

      store.trades.push(trade);
    });

    const store = await readStore();

    res.status(201).json({
      trade,
      state: buildState(store)
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/trades/:id", async (req, res, next) => {
  try {
    const { id } = req.params;

    let removed = false;
    await mutateStore((store) => {
      const before = store.trades.length;
      store.trades = store.trades.filter((item) => item.id !== id);
      removed = before !== store.trades.length;
    });

    if (!removed) {
      throw createHttpError(404, "未找到对应 trade id");
    }

    const store = await readStore();

    res.json({
      ok: true,
      state: buildState(store)
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/quotes/refresh", async (_req, res, next) => {
  try {
    const beforeStore = await readStore();
    const { positions } = buildPortfolio(beforeStore.trades, beforeStore.quotes);
    const symbols = positions.map((item) => item.apiSymbol);

    if (symbols.length === 0) {
      throw createHttpError(400, "没有可刷新行情的持仓");
    }

    const refreshResult = await refreshQuotes(symbols);

    await mutateStore((store) => {
      for (const [apiSymbol, quote] of Object.entries(refreshResult.quotesBySymbol)) {
        store.quotes[apiSymbol] = quote;
      }
      pushSnapshot(store, "quote_refresh");
    });

    const store = await readStore();

    res.json({
      refresh: refreshResult,
      state: buildState(store)
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/quotes/manual", async (req, res, next) => {
  try {
    const symbolRaw = String(req.body.symbol || "").trim().toUpperCase();
    const market = normalizeMarket(symbolRaw, req.body.market);
    const apiSymbol = toApiSymbol(symbolRaw, market);

    if (!apiSymbol) {
      throw createHttpError(400, "symbol 无效");
    }

    const price = toNumber(req.body.price);
    if (!price || price <= 0) {
      throw createHttpError(400, "price 必须大于 0");
    }

    const previousCloseRaw = req.body.previousClose;
    const previousCloseParsed =
      previousCloseRaw === "" || previousCloseRaw === null || typeof previousCloseRaw === "undefined"
        ? null
        : toNumber(previousCloseRaw);

    if (previousCloseParsed !== null && previousCloseParsed <= 0) {
      throw createHttpError(400, "previousClose 必须大于 0");
    }

    await mutateStore((store) => {
      store.quotes[apiSymbol] = {
        apiSymbol,
        lastPrice: price,
        previousClose: previousCloseParsed ?? price,
        currency: String(req.body.currency || "").trim().toUpperCase() || "",
        exchange: "",
        shortName: "",
        asOf: new Date().toISOString(),
        source: "manual"
      };

      pushSnapshot(store, "manual_quote");
    });

    const store = await readStore();

    res.json({
      ok: true,
      state: buildState(store)
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/snapshots", async (req, res, next) => {
  try {
    const days = Math.max(1, Math.min(3650, Number(req.query.days) || 180));
    const since = Date.now() - days * 24 * 60 * 60 * 1000;

    const store = await readStore();
    const snapshots = store.snapshots
      .filter((item) => new Date(item.timestamp).getTime() >= since)
      .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

    res.json({ snapshots });
  } catch (error) {
    next(error);
  }
});

app.post("/api/snapshots", async (req, res, next) => {
  try {
    let snapshot;

    await mutateStore((store) => {
      const sourceRaw = String(req.body.source || "manual").trim().toLowerCase();
      const source = sourceRaw || "manual";
      snapshot = pushSnapshot(store, source);
    });

    res.status(201).json({ snapshot });
  } catch (error) {
    next(error);
  }
});

app.get("/api/monthly-updates", async (_req, res, next) => {
  try {
    const store = await readStore();
    res.json(buildMonthlyUpdatesPayload(store));
  } catch (error) {
    next(error);
  }
});

app.post("/api/monthly-updates", async (req, res, next) => {
  try {
    const source = String(req.body.source || "both").trim().toLowerCase();
    if (!["xueqiu", "weibo", "both", "other"].includes(source)) {
      throw createHttpError(400, "source 只能是 xueqiu / weibo / both / other");
    }

    const postedAt = toDateIso(req.body.postedAt);
    if (!postedAt) {
      throw createHttpError(400, "postedAt 无效");
    }

    const month = String(req.body.month || toMonthKey(postedAt)).trim();
    if (!/^\d{4}-\d{2}$/.test(month)) {
      throw createHttpError(400, "month 格式必须为 YYYY-MM");
    }

    const note = String(req.body.note || "").trim();

    const update = {
      id: randomUUID(),
      month,
      source,
      postedAt,
      note,
      createdAt: new Date().toISOString()
    };

    await mutateStore((store) => {
      store.monthlyUpdates.push(update);
      if (store.monthlyUpdates.length > 240) {
        store.monthlyUpdates = store.monthlyUpdates.slice(-240);
      }
    });

    const store = await readStore();

    res.status(201).json({
      update,
      monthlyStatus: buildMonthlyStatus(store.monthlyUpdates || [])
    });
  } catch (error) {
    next(error);
  }
});

app.patch("/api/monthly-updates/:id", async (req, res, next) => {
  try {
    const { id } = req.params;
    const clearManualMetrics = Boolean(req.body?.clearManualMetrics);
    const netValueWan = clearManualMetrics ? undefined : parseNullableMetricNumber(req.body?.netValueWan, "累计净值(万)");
    const netIndex = clearManualMetrics ? undefined : parseNullableMetricNumber(req.body?.netIndex, "净值指数");
    const yearStartNetIndex =
      clearManualMetrics ? undefined : parseNullableMetricNumber(req.body?.yearStartNetIndex, "年初净值指数");
    const nextManualMetrics = clearManualMetrics
      ? null
      : normalizeManualMetrics({
          cumulativeNetValue: typeof netValueWan === "undefined" ? undefined : netValueWan * 10_000,
          netIndex,
          yearStartNetIndex
        });

    let updatedRecord = null;
    let updatedSnapshot = null;

    await mutateStore((store) => {
      const monthlyIndex = (store.monthlyUpdates || []).findIndex((item) => item.id === id);
      if (monthlyIndex < 0) {
        throw createHttpError(404, "未找到对应的月度记录");
      }

      const currentUpdate = store.monthlyUpdates[monthlyIndex];
      const snapshotIndex = findSnapshotIndexByMonthlyUpdate(store.masterSnapshots, currentUpdate);
      const snapshotRecord = snapshotIndex >= 0 ? store.masterSnapshots[snapshotIndex] : null;
      const snapshotMetrics = extractSnapshotPostMetrics(
        buildMetricsSourceSnapshot(snapshotRecord, nextManualMetrics)
      );
      const manualOnlyMetrics = extractManualOnlyMetrics(nextManualMetrics);
      const mergedMetrics = {
        cumulativeNetValue: snapshotMetrics.cumulativeNetValue ?? manualOnlyMetrics.cumulativeNetValue,
        netIndex: snapshotMetrics.netIndex ?? manualOnlyMetrics.netIndex,
        yearStartNetIndex: snapshotMetrics.yearStartNetIndex ?? manualOnlyMetrics.yearStartNetIndex
      };
      const updatedAt = new Date().toISOString();

      updatedRecord = {
        ...mergeMonthlyUpdateMetricFields(
          {
            ...currentUpdate,
            netValue: null,
            netIndex: null,
            yearStartNetIndex: null,
            manualMetrics: nextManualMetrics,
            updatedAt
          },
          mergedMetrics
        )
      };
      store.monthlyUpdates[monthlyIndex] = updatedRecord;

      if (snapshotIndex >= 0) {
        updatedSnapshot = {
          ...snapshotRecord,
          manualMetrics: nextManualMetrics,
          updatedAt,
          ...extractSnapshotPostMetrics(buildMetricsSourceSnapshot(snapshotRecord, nextManualMetrics))
        };
        store.masterSnapshots[snapshotIndex] = updatedSnapshot;
      }

      const autoTracking = ensureAutoTrackingState(store);
      if (autoTracking.latestSnapshot) {
        const latestPostId = normalizePostId(autoTracking.latestSnapshot.postId);
        const targetPostId = normalizePostId(currentUpdate.postId);
        const sameLatestPost =
          latestPostId &&
          targetPostId &&
          latestPostId === targetPostId;

        if (sameLatestPost) {
          autoTracking.latestSnapshot = {
            ...autoTracking.latestSnapshot,
            manualMetrics: nextManualMetrics,
            updatedAt,
            ...extractSnapshotPostMetrics(buildMetricsSourceSnapshot(autoTracking.latestSnapshot, nextManualMetrics))
          };
        } else if (updatedSnapshot && normalizePostId(updatedSnapshot.postId) === latestPostId) {
          autoTracking.latestSnapshot = updatedSnapshot;
        }
      }

      if (!autoTracking.latestSnapshot && updatedSnapshot) {
        autoTracking.latestSnapshot = updatedSnapshot;
      }
    });

    const store = await readStore();
    res.json({
      update: updatedRecord,
      snapshot: updatedSnapshot,
      monthlyStatus: buildMonthlyStatus(store.monthlyUpdates || [])
    });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/monthly-updates/:id", async (req, res, next) => {
  try {
    const { id } = req.params;

    let removed = false;
    await mutateStore((store) => {
      const before = store.monthlyUpdates.length;
      store.monthlyUpdates = store.monthlyUpdates.filter((item) => item.id !== id);
      removed = before !== store.monthlyUpdates.length;
    });

    if (!removed) {
      throw createHttpError(404, "未找到对应 month update id");
    }

    const store = await readStore();

    res.json({
      ok: true,
      monthlyStatus: buildMonthlyStatus(store.monthlyUpdates || [])
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/auto-tracking", async (_req, res, next) => {
  try {
    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);
    const latestSnapshot = sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null);

    res.json({
      autoTracking: getAutoTrackingPublic(autoTracking, { latestSnapshot }),
      latestSnapshot
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/auto-tracking/bootstrap", async (req, res, next) => {
  try {
    const store = await readStore();
    res.json(
      buildAutoTrackingBootstrapPayload(store, {
        limit: req.query.limit
      })
    );
  } catch (error) {
    next(error);
  }
});

app.post("/api/auto-tracking/config", async (req, res, next) => {
  try {
    const payload = req.body || {};

    await mutateStore((store) => {
      const autoTracking = ensureAutoTrackingState(store);
      const patch = buildAutoTrackingConfigPatch(autoTracking.config, payload);
      autoTracking.config = mergeAutoTrackingConfig(patch);
    });

    invalidateCatalogCache();
    await scheduleAutoTracking();
    await scheduleCookieKeepAlive();

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      ok: true,
      autoTracking: getAutoTrackingPublic(autoTracking)
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auto-tracking/run", async (_req, res, next) => {
  try {
    const { result, job } = await runAutoTrackingWithJob("manual", {}, {
      label: "立即抓取",
      startMessage: "正在抓取最新帖子",
      successMessage: "抓取完成"
    });
    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      job,
      result,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    next(error);
  }
});
app.post("/api/auto-tracking/cookie-keepalive", async (_req, res, next) => {
  const job = createJob("cookie_keepalive", { label: "Cookie 保活" });
  try {
    startJob(job.jobId, { stage: "keepalive", progress: 10, message: "正在保活 Cookie" });
    const result = await runCookieKeepAliveJob("manual");
    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    if (result.skipped) {
      skipJob(job.jobId, result.reason || "Cookie 保活已跳过");
    } else if (result.ok) {
      finishJob(job.jobId, {
        summary: {
          ok: true,
          successCount: Number(result.successCount) || 0,
          failedCount: Number(result.failedCount) || 0,
          skippedCount: Number(result.skippedCount) || 0
        },
        logs: result.logs,
        message: "Cookie 保活完成"
      });
    } else {
      failJob(job.jobId, new Error(result.error || "Cookie 保活失败"), {
        stage: "keepalive",
        summary: { ok: false }
      });
    }

    res.json({
      ok: true,
      job: getJob(job.jobId),
      result,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    failJob(job.jobId, error, { stage: "keepalive" });
    next(error);
  }
});
app.post("/api/auto-tracking/backfill", async (req, res, next) => {
  try {
    const { pages, pageSize } = normalizeBackfillInput(req.body || {});

    const { result, job } = await runAutoTrackingWithJob("backfill", {
      mode: "backfill",
      backfillPages: pages,
      backfillPageSize: pageSize
    }, {
      label: "历史回溯",
      startMessage: "正在回溯历史帖子",
      successMessage: "历史回溯完成"
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      job,
      result,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    next(error);
  }
});
app.post("/api/auto-tracking/catalog", async (req, res, next) => {
  try {
    const pagesRaw = req.body?.pages;
    const pageSizeRaw = req.body?.pageSize;
    const pages =
      typeof pagesRaw === "undefined" || pagesRaw === null || pagesRaw === ""
        ? undefined
        : Number(pagesRaw);
    const pageSize =
      typeof pageSizeRaw === "undefined" || pageSizeRaw === null || pageSizeRaw === ""
        ? undefined
        : Number(pageSizeRaw);

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);
    const config = mergeAutoTrackingConfig(autoTracking.config);

    const { catalog, cacheHit } = await collectSuperLudinggongPostCatalogCached(config, {
      backfillPages: pages,
      backfillPageSize: pageSize
    });

    const importedPostIds = new Set((store.masterSnapshots || []).map((item) => item.postId));
    const processedPostIds = new Set(autoTracking.processedPostIds || []);

    const posts = catalog.posts.map((item) => ({
      ...item,
      imported: importedPostIds.has(item.postId),
      processed: processedPostIds.has(item.postId)
    }));

    res.json({
      posts,
      logs: cacheHit
        ? [
            {
              id: `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`,
              createdAt: new Date().toISOString(),
              level: "info",
              message: `目录缓存命中：复用 ${Math.round(CATALOG_CACHE_TTL_MS / 1000)} 秒内结果`
            },
            ...(catalog.logs || [])
          ]
        : catalog.logs,
      cached: cacheHit
    });
  } catch (error) {
    next(error);
  }
});


app.post("/api/auto-tracking/import-post-url", async (req, res, next) => {
  let job = null;
  try {
    const { postIds } = normalizeSelectedImportInput({ postIds: [req.body?.postUrl || req.body?.url || req.body?.postId] });
    if (postIds.length === 0) {
      throw createHttpError(400, "请输入有效的雪球帖子链接或帖子 ID");
    }

    job = createJob("auto_tracking_import_post_url", { label: "导入指定帖子" });
    startJob(job.jobId, { stage: "import", progress: 10, message: "正在导入指定帖子" });
    const result = await runAutoTrackingJob("import_post_url", {
      mode: "backfill",
      targetPostIds: postIds,
      forceRefresh: true,
      backfillPages: 1,
      backfillPageSize: 20
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    const classification = classifyAutoTrackingResult(result, {
      targeted: true,
      actionLabel: "指定帖子导入"
    });

    if (classification.status === "skipped") {
      skipJob(job.jobId, result.reason || "导入已跳过");
    } else if (classification.status === "succeeded") {
      finishJob(job.jobId, {
        summary: summarizeAutoTrackingResult(result),
        logs: result.logs,
        message: "指定帖子导入完成"
      });
    } else {
      failJob(job.jobId, new Error(classification.message || result.error || result.reason || "导入失败"), {
        stage: "import",
        summary: summarizeAutoTrackingResult(result)
      });
    }

    res.json({
      job: getJob(job.jobId),
      result,
      postIds,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    if (job) {
      failJob(job.jobId, error, { stage: "import" });
    }
    next(error);
  }
});

app.post("/api/auto-tracking/import-selected", async (req, res, next) => {
  let job = null;
  try {
    const { postIds, pages, pageSize } = normalizeSelectedImportInput(req.body || {});
    if (postIds.length === 0) {
      throw createHttpError(400, "postIds 不能为空");
    }

    job = createJob("auto_tracking_import_selected", { label: "导入选中月份" });
    startJob(job.jobId, { stage: "import", progress: 10, message: "正在导入选中月份" });
    const result = await runAutoTrackingJob("import_selected", {
      mode: "backfill",
      targetPostIds: postIds,
      forceRefresh: true,
      backfillPages: pages,
      backfillPageSize: pageSize
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    if (result.skipped) {
      skipJob(job.jobId, result.reason || "导入已跳过");
    } else if (result.ok) {
      finishJob(job.jobId, {
        summary: summarizeAutoTrackingResult(result),
        logs: result.logs,
        message: "选中月份导入完成"
      });
    } else {
      failJob(job.jobId, new Error(result.error || result.reason || "导入失败"), {
        stage: "import",
        summary: summarizeAutoTrackingResult(result)
      });
    }

    res.json({
      job: getJob(job.jobId),
      result,
      selectedCount: postIds.length,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    if (job) {
      failJob(job.jobId, error, { stage: "import" });
    }
    next(error);
  }
});
app.get("/api/auto-tracking/anomalies", async (_req, res, next) => {
  try {
    const store = await readStore();
    const snapshotsWithDiagnostics = sanitizeSnapshotCollection(store.masterSnapshots || [], {
      includeDiagnostics: true
    });
    res.json(buildAnomalyReportFromSnapshots(snapshotsWithDiagnostics));
  } catch (error) {
    next(error);
  }
});

app.post("/api/auto-tracking/recalculate-snapshots", async (_req, res, next) => {
  try {
    let summary = {
      snapshotCount: 0,
      changedSnapshotCount: 0,
      changedRowCount: 0,
      issueRowCount: 0
    };

    const beforeStore = await readStore();
    const beforeAutoTracking = ensureAutoTrackingState(beforeStore);
    const apiSymbols = collectSnapshotApiSymbols(beforeStore.masterSnapshots || []);
    let securityNames = {
      namesBySymbol: {},
      sourcesBySymbol: {}
    };

    try {
      securityNames = await lookupSecurityNames(apiSymbols, {
        xueqiuCookie: beforeAutoTracking.config?.xueqiuCookie
      });
    } catch (_error) {
      securityNames = {
        namesBySymbol: {},
        sourcesBySymbol: {}
      };
    }

    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      const nextSnapshots = [];
      const nameStats = buildNameValidationSourceStats(securityNames.namesBySymbol, securityNames.sourcesBySymbol);
      const originalRowsBySnapshotKey = new Map(
        (draft.masterSnapshots || []).map((snapshot, index) => [
          String(snapshot?.id || snapshot?.postId || index),
          Array.isArray(snapshot?.rows) ? structuredClone(snapshot.rows) : []
        ])
      );

      applyCollectedSecurityNameValidation(
        draft.masterSnapshots || [],
        securityNames.namesBySymbol,
        securityNames.sourcesBySymbol
      );

      for (const trade of draft.trades || []) {
        const apiSymbol = String(trade?.apiSymbol || "").trim().toUpperCase();
        const validatedName = String(securityNames.namesBySymbol?.[apiSymbol] || "").trim();
        if (!validatedName) {
          continue;
        }

        const nameSource = String(securityNames.sourcesBySymbol?.[apiSymbol] || "").trim().toLowerCase();
        trade.name = normalizeSecurityName(trade.symbol || apiSymbol, validatedName, trade.market, {
          nameSource
        });
        trade.nameSource = nameSource;
      }

      for (const snapshot of draft.masterSnapshots || []) {
        const snapshotKey = String(snapshot?.id || snapshot?.postId || nextSnapshots.length);
        const originalRows = Array.isArray(originalRowsBySnapshotKey.get(snapshotKey))
          ? originalRowsBySnapshotKey.get(snapshotKey)
          : [];
        const sanitized = sanitizeMasterSnapshotRecord(snapshot, { includeDiagnostics: true });
        const cleanRows = sanitized.rows.map((row) => {
          const { diagnostics, ...rest } = row;
          return rest;
        });
        let changedRowsInSnapshot = 0;
        const rowCompareLength = Math.max(originalRows.length, cleanRows.length);

        for (let index = 0; index < rowCompareLength; index += 1) {
          const originalSerialized = JSON.stringify(originalRows[index] || null);
          const cleanSerialized = JSON.stringify(cleanRows[index] || null);
          if (originalSerialized !== cleanSerialized) {
            changedRowsInSnapshot += 1;
          }
        }

        summary.snapshotCount += 1;
        summary.changedRowCount += changedRowsInSnapshot;
        summary.issueRowCount += cleanRows.reduce((count, _row, index) => {
          const diagnostics = sanitized.rows[index]?.diagnostics;
          return count + (diagnostics && diagnostics.issues.length > 0 ? 1 : 0);
        }, 0);

        if (changedRowsInSnapshot > 0) {
          summary.changedSnapshotCount += 1;
        }

        nextSnapshots.push({
          ...snapshot,
          rows: cleanRows,
          updatedAt:
            changedRowsInSnapshot > 0
              ? new Date().toISOString()
              : snapshot.updatedAt || snapshot.createdAt || new Date().toISOString()
        });
      }

      draft.masterSnapshots = sortByRecentDate(nextSnapshots, "postedAt").slice(0, 200);
      autoTracking.latestSnapshot = draft.masterSnapshots[0] || null;
      const nameLog = buildNameValidationLog(nameStats, "历史名称重算");
      if (nameLog) {
        appendAutoTrackingLogs(autoTracking, [nameLog]);
      }
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      ok: true,
      summary,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/master-snapshots", async (req, res, next) => {
  try {
    const limit = Math.max(1, Math.min(240, Number(req.query.limit) || 10));
    const store = await readStore();
    const snapshots = sanitizeSnapshotCollection((store.masterSnapshots || []).slice(0, limit));

    res.json({ snapshots });
  } catch (error) {
    next(error);
  }
});

app.get("/api/master-snapshots/:id", async (req, res, next) => {
  try {
    const targetId = String(req.params.id || "").trim();
    if (!targetId) {
      throw createHttpError(400, "snapshot id 不能为空");
    }

    const store = await readStore();
    const snapshot = (store.masterSnapshots || []).find((item) => String(item?.id || "").trim() === targetId);
    if (!snapshot) {
      throw createHttpError(404, "未找到对应快照");
    }

    res.json({
      snapshot: sanitizeMasterSnapshotRecord(snapshot)
    });
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  const status = error.status || 500;
  res.status(status).json({
    error: error.message || "服务器异常"
  });
});

ensureStore()
  .then(async () => {
    await backfillStoredPostMetrics();
    await scheduleAutoTracking();
    await scheduleCookieKeepAlive();
    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);
    const startupDecision = shouldRunAutoTrackingOnStartup(autoTracking, new Date());
    if (startupDecision.shouldRun) {
      runAutoTrackingWithJob("startup", {}, {
        label: "启动抓取",
        startMessage: "服务启动后正在检查最新帖子",
        successMessage: "启动抓取完成"
      }).catch((error) => {
        console.error("Auto tracking startup error:", error.message);
      });
    } else {
      console.log(`Auto tracking startup skipped: ${startupDecision.reason}`);
    }

    const cookieKeepAliveStartupDecision = shouldRunCookieKeepAliveOnStartup(autoTracking, new Date());
    if (cookieKeepAliveStartupDecision.shouldRun) {
      runCookieKeepAliveJob("startup").catch((error) => {
        console.error("Cookie keep-alive startup error:", error.message);
      });
    } else {
      console.log(`Cookie keep-alive startup skipped: ${cookieKeepAliveStartupDecision.reason}`);
    }

    app.listen(PORT, (error) => {
      if (error) {
        throw error;
      }
      console.log(`Stock tracker running at http://localhost:${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Failed to start server:", error);
    process.exit(1);
  });
