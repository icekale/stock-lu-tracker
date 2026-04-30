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

function toBooleanIfPresent(payload, key, target) {
  if (typeof payload[key] !== "undefined") {
    target[key] = Boolean(payload[key]);
  }
}

function toNumberIfPresent(payload, key, target) {
  if (typeof payload[key] !== "undefined") {
    target[key] = Number(payload[key]);
  }
}

function toStringIfPresent(payload, key, target) {
  if (typeof payload[key] !== "undefined") {
    target[key] = String(payload[key] || "").trim();
  }
}

function splitList(value, splitter) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  return String(value || "")
    .split(splitter)
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildAutoTrackingConfigPatch(currentConfig = {}, payload = {}) {
  const patch = { ...currentConfig };

  for (const key of [
    "enabled",
    "smartScheduleEnabled",
    "skipStartupOutsideWindow",
    "cookieKeepAliveEnabled",
    "ocrEnabled"
  ]) {
    toBooleanIfPresent(payload, key, patch);
  }

  for (const key of [
    "intervalMinutes",
    "monthEndWindowDays",
    "offWindowIntervalHours",
    "cookieKeepAliveIntervalHours",
    "maxPostsPerSource",
    "ocrMaxImagesPerPost",
    "backfillMaxPages",
    "backfillPageSize"
  ]) {
    toNumberIfPresent(payload, key, patch);
  }

  for (const key of ["ocrProvider", "xueqiuTitleRegex"]) {
    toStringIfPresent(payload, key, patch);
  }

  if (typeof payload.keywords !== "undefined") {
    patch.keywords = splitList(payload.keywords, /[,\n]/);
  }

  if (typeof payload.pinnedPostUrls !== "undefined") {
    patch.pinnedPostUrls = splitList(payload.pinnedPostUrls, /[\n,]/);
  }

  patch.xueqiuCookie = readPatchedSecretValue(payload, "xueqiuCookie", "clearXueqiuCookie", patch.xueqiuCookie);
  patch.weiboCookie = readPatchedSecretValue(payload, "weiboCookie", "clearWeiboCookie", patch.weiboCookie);
  patch.qwenApiKey = readPatchedSecretValue(payload, "qwenApiKey", "clearQwenApiKey", patch.qwenApiKey);

  return patch;
}

function optionalNumber(value) {
  if (typeof value === "undefined" || value === null || value === "") {
    return undefined;
  }
  return Number(value);
}

function normalizeBackfillInput(payload = {}) {
  return {
    pages: optionalNumber(payload.pages),
    pageSize: optionalNumber(payload.pageSize)
  };
}

function normalizePostIds(input) {
  if (!Array.isArray(input)) {
    return [];
  }
  return [
    ...new Set(
      input
        .map((item) => String(item || "").trim())
        .filter((item) => /^xq:\d{6,}$/i.test(item) || /^wb:[A-Za-z0-9]{6,}$/i.test(item))
    )
  ];
}

function normalizeSelectedImportInput(payload = {}) {
  const backfill = normalizeBackfillInput(payload);
  return {
    postIds: normalizePostIds(payload.postIds),
    pages: backfill.pages,
    pageSize: backfill.pageSize
  };
}

function summarizeAutoTrackingResult(result = {}) {
  return {
    ok: Boolean(result.ok),
    mode: String(result.mode || "normal"),
    importedSnapshots: Number(result.importedSnapshots) || 0,
    importedTrades: Number(result.importedTrades) || 0,
    skippedSnapshots: Number(result.skippedSnapshots) || 0,
    logCount: Array.isArray(result.logs) ? result.logs.length : 0,
    error: String(result.error || "")
  };
}

module.exports = {
  buildAutoTrackingConfigPatch,
  summarizeAutoTrackingResult,
  normalizeBackfillInput,
  normalizeSelectedImportInput,
  normalizePostIds,
  readPatchedSecretValue
};
