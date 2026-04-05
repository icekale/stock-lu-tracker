const path = require("node:path");
const { createHash, createHmac, randomUUID, timingSafeEqual } = require("node:crypto");

const express = require("express");
const appPackage = require("../package.json");

const { ensureStore, readStore, mutateStore } = require("./store");
const { buildPortfolio, buildMonthlyStatus, toMonthKey } = require("./portfolio");
const { extractSnapshotPostMetrics } = require("./post-metrics");
const { refreshQuotes } = require("./quotes");
const { toApiSymbol, normalizeMarket, normalizeSecurityName } = require("./symbols");
const {
  ensureAutoTrackingState,
  mergeAutoTrackingConfig,
  collectSuperLudinggongSnapshots,
  collectSuperLudinggongPostCatalog
} = require("./super-ludinggong-sync");

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

let autoTrackingRunning = false;
let autoTrackingTimer = null;

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
app.use(express.static(path.join(process.cwd(), "public")));

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
    name: normalizeSecurityName(symbol, payload.name, market),
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
    name: normalizeSecurityName(row.symbol, row.name),
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
    clearInterval(autoTrackingTimer);
    autoTrackingTimer = null;
  }

  const store = await readStore();
  const autoTracking = ensureAutoTrackingState(store);
  const config = mergeAutoTrackingConfig(autoTracking.config);

  if (!config.enabled) {
    await mutateStore((draft) => {
      const state = ensureAutoTrackingState(draft);
      state.runtime.nextRunAt = null;
    });
    return;
  }

  const intervalMs = config.intervalMinutes * 60 * 1000;
  autoTrackingTimer = setInterval(() => {
    runAutoTrackingJob("timer").catch((error) => {
      console.error("Auto tracking timer error:", error.message);
    });
  }, intervalMs);

  await mutateStore((draft) => {
    const state = ensureAutoTrackingState(draft);
    state.runtime.nextRunAt = new Date(Date.now() + intervalMs).toISOString();
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

    let importedSnapshots = 0;
    let importedTrades = 0;
    let skippedSnapshots = 0;

    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      const processedPostIds = new Set(autoTracking.processedPostIds || []);
      const importedTradeKeys = new Set(autoTracking.importedTradeKeys || []);
      const quantityBySymbol = buildPositionQuantityIndex(draft.trades, draft.quotes);

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

      const intervalMs = mergeAutoTrackingConfig(autoTracking.config).intervalMinutes * 60 * 1000;
      autoTracking.runtime.nextRunAt = autoTracking.config.enabled
        ? new Date(Date.now() + intervalMs).toISOString()
        : null;
    });

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
  if (!Array.isArray(input)) {
    return [];
  }

  const ids = input
    .map((item) => String(item || "").trim())
    .filter((item) => /^xq:\d{6,}$/i.test(item) || /^wb:[A-Za-z0-9]{6,}$/i.test(item));

  return [...new Set(ids)];
}

function buildMonthlyUpdatesPayload(store) {
  return {
    updates: sortByRecentDate(store.monthlyUpdates || [], "postedAt"),
    monthlyStatus: buildMonthlyStatus(store.monthlyUpdates || []),
    links: PROFILE_LINKS
  };
}

function buildAutoTrackingBootstrapPayload(store, options = {}) {
  const autoTracking = ensureAutoTrackingState(store);
  const limit = Math.max(1, Math.min(240, Number(options.limit) || 240));
  const snapshotSource = (store.masterSnapshots || []).slice(0, limit);
  const snapshotsWithDiagnostics = sanitizeSnapshotCollection(snapshotSource, { includeDiagnostics: true });
  const snapshots = snapshotsWithDiagnostics.map((snapshot) => stripSnapshotDiagnostics(snapshot));
  const latestSnapshot = snapshots[0] || sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || null);

  return {
    autoTracking: getAutoTrackingPublic(autoTracking, {
      includeLatestSnapshot: false
    }),
    latestSnapshot,
    snapshots,
    monthlyUpdates: buildMonthlyUpdatesPayload(store),
    anomalyReport: buildAnomalyReportFromSnapshots(snapshotsWithDiagnostics)
  };
}

app.get("/api/health", (_req, res) => {
  res.json({
    ok: true,
    now: new Date().toISOString()
  });
});

app.get("/api/app-meta", (_req, res) => {
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

      const patch = {
        ...autoTracking.config
      };

      if (typeof payload.enabled !== "undefined") {
        patch.enabled = Boolean(payload.enabled);
      }

      if (typeof payload.intervalMinutes !== "undefined") {
        patch.intervalMinutes = Number(payload.intervalMinutes);
      }

      if (typeof payload.maxPostsPerSource !== "undefined") {
        patch.maxPostsPerSource = Number(payload.maxPostsPerSource);
      }

      if (typeof payload.ocrEnabled !== "undefined") {
        patch.ocrEnabled = Boolean(payload.ocrEnabled);
      }

      if (typeof payload.ocrProvider !== "undefined") {
        patch.ocrProvider = String(payload.ocrProvider || "").trim();
      }

      if (typeof payload.ocrMaxImagesPerPost !== "undefined") {
        patch.ocrMaxImagesPerPost = Number(payload.ocrMaxImagesPerPost);
      }

      if (typeof payload.keywords !== "undefined") {
        patch.keywords = Array.isArray(payload.keywords)
          ? payload.keywords
          : String(payload.keywords || "")
              .split(/[,\n]/)
              .map((item) => item.trim())
              .filter(Boolean);
      }

      if (typeof payload.pinnedPostUrls !== "undefined") {
        patch.pinnedPostUrls = Array.isArray(payload.pinnedPostUrls)
          ? payload.pinnedPostUrls
          : String(payload.pinnedPostUrls || "")
              .split(/[\n,]/)
              .map((item) => item.trim())
              .filter(Boolean);
      }

      if (typeof payload.xueqiuTitleRegex !== "undefined") {
        patch.xueqiuTitleRegex = String(payload.xueqiuTitleRegex || "").trim();
      }

      if (typeof payload.backfillMaxPages !== "undefined") {
        patch.backfillMaxPages = Number(payload.backfillMaxPages);
      }

      if (typeof payload.backfillPageSize !== "undefined") {
        patch.backfillPageSize = Number(payload.backfillPageSize);
      }

      if (typeof payload.xueqiuCookie === "string") {
        patch.xueqiuCookie = payload.xueqiuCookie.trim();
      }

      if (typeof payload.weiboCookie === "string") {
        patch.weiboCookie = payload.weiboCookie.trim();
      }

      if (typeof payload.qwenApiKey === "string") {
        patch.qwenApiKey = payload.qwenApiKey.trim();
      }

      autoTracking.config = mergeAutoTrackingConfig(patch);
    });

    await scheduleAutoTracking();

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
    const result = await runAutoTrackingJob("manual");
    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      result,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auto-tracking/backfill", async (req, res, next) => {
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

    const result = await runAutoTrackingJob("backfill", {
      mode: "backfill",
      backfillPages: pages,
      backfillPageSize: pageSize
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
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

    const catalog = await collectSuperLudinggongPostCatalog(config, {
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
      logs: catalog.logs
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/auto-tracking/import-selected", async (req, res, next) => {
  try {
    const postIds = normalizePostIds(req.body?.postIds);
    if (postIds.length === 0) {
      throw createHttpError(400, "postIds 不能为空");
    }

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

    const result = await runAutoTrackingJob("import_selected", {
      mode: "backfill",
      targetPostIds: postIds,
      forceRefresh: true,
      backfillPages: pages,
      backfillPageSize: pageSize
    });

    const store = await readStore();
    const autoTracking = ensureAutoTrackingState(store);

    res.json({
      result,
      selectedCount: postIds.length,
      autoTracking: getAutoTrackingPublic(autoTracking),
      latestSnapshot: sanitizeMasterSnapshotRecord(autoTracking.latestSnapshot || store.masterSnapshots?.[0] || null)
    });
  } catch (error) {
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

    await mutateStore((draft) => {
      const autoTracking = ensureAutoTrackingState(draft);
      const nextSnapshots = [];

      for (const snapshot of draft.masterSnapshots || []) {
        const sanitized = sanitizeMasterSnapshotRecord(snapshot, { includeDiagnostics: true });
        const cleanRows = sanitized.rows.map((row) => {
          const { diagnostics, ...rest } = row;
          return rest;
        });
        const changed = JSON.stringify(snapshot.rows || []) !== JSON.stringify(cleanRows);

        summary.snapshotCount += 1;
        summary.changedRowCount += cleanRows.reduce((count, _row, index) => {
          const diagnostics = sanitized.rows[index]?.diagnostics;
          return count + (diagnostics && diagnostics.fixes.length > 0 ? 1 : 0);
        }, 0);
        summary.issueRowCount += cleanRows.reduce((count, _row, index) => {
          const diagnostics = sanitized.rows[index]?.diagnostics;
          return count + (diagnostics && diagnostics.issues.length > 0 ? 1 : 0);
        }, 0);

        if (changed) {
          summary.changedSnapshotCount += 1;
        }

        nextSnapshots.push({
          ...snapshot,
          rows: cleanRows,
          updatedAt: changed ? new Date().toISOString() : snapshot.updatedAt || snapshot.createdAt || new Date().toISOString()
        });
      }

      draft.masterSnapshots = sortByRecentDate(nextSnapshots, "postedAt").slice(0, 200);
      autoTracking.latestSnapshot = draft.masterSnapshots[0] || null;
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
    runAutoTrackingJob("startup").catch((error) => {
      console.error("Auto tracking startup error:", error.message);
    });

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
