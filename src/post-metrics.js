const POST_METRIC_OVERRIDES = Object.freeze({
  // Snowball post bodies are sometimes truncated by the API. These values come
  // from the corresponding post text that the user already verified.
  "xq:381996320": {
    netIndex: 10.9972,
    yearStartNetIndex: 10.023
  },
  "xq:377251650": {
    netIndex: 10.7384,
    yearStartNetIndex: 10.023
  },
  "xq:374028772": {
    netIndex: 10.5885,
    yearStartNetIndex: 10.023
  }
});

function toNumber(value) {
  if (value === null || typeof value === "undefined") {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function roundNumeric(value, digits = 3) {
  const number = toNumber(value);
  if (number === null) {
    return null;
  }
  const factor = 10 ** digits;
  return Math.round(number * factor) / factor;
}

function normalizeStatsText(text) {
  return String(text || "")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toYuanByUnit(value, unit, hintText = "") {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) {
    return null;
  }

  const marker = String(unit || "").trim();
  if (/^[Ww万]$/.test(marker)) {
    return number * 10_000;
  }
  if (marker === "亿") {
    return number * 100_000_000;
  }

  if (!marker && number < 100_000 && /(?:收盘|净值)/.test(hintText)) {
    return number * 10_000;
  }

  return number;
}

function parseCumulativeNetValue(text) {
  const normalized = normalizeStatsText(text);
  if (!normalized) {
    return null;
  }

  const patterns = [
    /(?:累积|累计)\s*净值(?:为|[:：])?\s*([0-9]+(?:\.[0-9]+)?)\s*([Ww万亿]?)/,
    /(?:本游戏仓)?\s*\d{1,2}\s*月\s*收盘\s*([0-9]+(?:\.[0-9]+)?)\s*([Ww万亿]?)/,
    /(?:本游戏仓)?\s*收盘\s*([0-9]+(?:\.[0-9]+)?)\s*([Ww万亿]?)/
  ];

  for (const pattern of patterns) {
    const matched = normalized.match(pattern);
    if (!matched) {
      continue;
    }

    const value = toYuanByUnit(matched[1], matched[2], matched[0]);
    if (value === null) {
      continue;
    }

    if (value < 1_000_000 || value > 500_000_000) {
      continue;
    }

    return value;
  }

  return null;
}

function parseYearStartIndex(text) {
  const normalized = normalizeStatsText(text);
  if (!normalized) {
    return null;
  }

  const matched = normalized.match(/(?:本年度初|年度初|年初)\s*净值指数(?:为|[:：])?\s*([0-9]+(?:\.[0-9]+)?)/);
  const value = toNumber(matched?.[1]);
  if (value === null || value <= 0 || value > 100_000) {
    return null;
  }

  return value;
}

function parseYearStartNetValue(text) {
  const normalized = normalizeStatsText(text);
  if (!normalized) {
    return null;
  }

  const matched = normalized.match(/(?:相比)?(?:本年初|年初)\s*([0-9]+(?:\.[0-9]+)?)\s*([Ww万亿]?)/);
  const value = toYuanByUnit(matched?.[1], matched?.[2], matched?.[0]);
  if (value === null || value < 1_000_000 || value > 500_000_000) {
    return null;
  }

  return value;
}

function parseNetIndex(text) {
  const normalized = normalizeStatsText(text);
  if (!normalized) {
    return null;
  }

  const removedYearStart = normalized.replace(
    /(?:本年度初|年度初|年初)\s*净值指数(?:为|[:：])?\s*[0-9]+(?:\.[0-9]+)?/g,
    " "
  );
  const matched =
    removedYearStart.match(/(?:本游戏仓)?\s*净值指数(?:为|[:：])?\s*([0-9]+(?:\.[0-9]+)?)/) ||
    removedYearStart.match(/净值指数\s*([0-9]+(?:\.[0-9]+)?)/);
  const value = toNumber(matched?.[1]);
  if (value === null || value <= 0 || value > 100_000) {
    return null;
  }

  return value;
}

function readPositiveMetricValue(primary, secondary) {
  const direct = toNumber(primary);
  if (direct !== null && direct > 0) {
    return direct;
  }

  const fallback = toNumber(secondary);
  if (fallback !== null && fallback > 0) {
    return fallback;
  }

  return null;
}

function deriveCurrentNetIndex(cumulativeNetValue, yearStartNetValue, yearStartNetIndex) {
  const currentValue = toNumber(cumulativeNetValue);
  const startValue = toNumber(yearStartNetValue);
  const startIndex = toNumber(yearStartNetIndex);
  if (currentValue === null || startValue === null || startIndex === null) {
    return null;
  }
  if (currentValue <= 0 || startValue <= 0 || startIndex <= 0) {
    return null;
  }

  return (currentValue / startValue) * startIndex;
}

function extractSnapshotPostMetrics(snapshot) {
  if (!snapshot) {
    return {
      cumulativeNetValue: null,
      netIndex: null,
      yearStartNetIndex: null
    };
  }

  const manual = snapshot.manualMetrics && typeof snapshot.manualMetrics === "object" ? snapshot.manualMetrics : {};
  const rawText = normalizeStatsText(snapshot.rawText);
  const mergedText = normalizeStatsText(`${String(snapshot.rawText || "")}\n${String(snapshot.ocrText || "")}`);
  const primaryText = rawText || mergedText;
  const override = POST_METRIC_OVERRIDES[String(snapshot.postId || "").trim()] || {};

  const cumulativeNetValue =
      readPositiveMetricValue(manual.cumulativeNetValue, snapshot.cumulativeNetValue) ??
      readPositiveMetricValue(snapshot.cumulativeNetValue, override.cumulativeNetValue) ??
      parseCumulativeNetValue(primaryText) ??
      parseCumulativeNetValue(mergedText);
  const yearStartNetIndex =
    readPositiveMetricValue(manual.yearStartNetIndex, snapshot.yearStartNetIndex) ??
    readPositiveMetricValue(snapshot.yearStartNetIndex, override.yearStartNetIndex) ??
    parseYearStartIndex(primaryText) ??
    parseYearStartIndex(mergedText) ??
    10.023;
  const yearStartNetValue = parseYearStartNetValue(primaryText) ?? parseYearStartNetValue(mergedText);
  const netIndex =
    readPositiveMetricValue(manual.netIndex, snapshot.netIndex) ??
    readPositiveMetricValue(snapshot.netIndex, override.netIndex) ??
    parseNetIndex(primaryText) ??
    parseNetIndex(mergedText) ??
    deriveCurrentNetIndex(cumulativeNetValue, yearStartNetValue, yearStartNetIndex);

  return {
    cumulativeNetValue,
    netIndex: roundNumeric(netIndex, 4),
    yearStartNetIndex: roundNumeric(yearStartNetIndex, 4)
  };
}

module.exports = {
  extractSnapshotPostMetrics
};
