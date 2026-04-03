const SYMBOL_NAME_OVERRIDES = Object.freeze({
  "00700": "腾讯控股"
});

function cleanRawSymbol(raw) {
  return String(raw || "")
    .trim()
    .toUpperCase()
    .replace(/\.(SS|SH|SZ|HK)$/, "")
    .replace(/^SH/, "")
    .replace(/^SZ/, "")
    .replace(/^HK/, "");
}

function inferMarket(rawSymbol) {
  const symbol = cleanRawSymbol(rawSymbol);

  if (/^\d{6}$/.test(symbol)) {
    return "CN";
  }

  if (/^\d{4,5}$/.test(symbol)) {
    return "HK";
  }

  return "US";
}

function toApiSymbol(rawSymbol, marketInput) {
  if (!rawSymbol) {
    return "";
  }

  const original = String(rawSymbol).trim().toUpperCase();

  if (/\.(SS|SZ|HK)$/.test(original)) {
    return original;
  }

  const market = (marketInput || inferMarket(original)).toUpperCase();
  const symbol = cleanRawSymbol(original);

  if (market === "CN") {
    if (!/^\d{6}$/.test(symbol)) {
      return original;
    }
    const suffix = symbol.startsWith("6") || symbol.startsWith("9") ? "SS" : "SZ";
    return `${symbol}.${suffix}`;
  }

  if (market === "HK") {
    if (!/^\d{4,5}$/.test(symbol)) {
      return original;
    }
    return `${symbol.padStart(4, "0")}.HK`;
  }

  return original.replace(".", "-");
}

function normalizeMarket(rawSymbol, marketInput) {
  if (marketInput) {
    return String(marketInput).toUpperCase();
  }
  return inferMarket(rawSymbol);
}

function cleanSecurityName(rawName) {
  return String(rawName || "")
    .replace(/[\s|｜]+/g, " ")
    .trim();
}

function normalizeSecurityName(rawSymbol, rawName, marketInput) {
  const market = normalizeMarket(rawSymbol, marketInput);
  const normalizedSymbol = cleanRawSymbol(toApiSymbol(rawSymbol, market) || rawSymbol);

  if (normalizedSymbol && SYMBOL_NAME_OVERRIDES[normalizedSymbol]) {
    return SYMBOL_NAME_OVERRIDES[normalizedSymbol];
  }

  const cleanedName = cleanSecurityName(rawName);
  const compactName = cleanedName.replace(/\s+/g, "");

  if (/^时(?:暑)?讯控股$/.test(compactName) || /^腾[讯汛迅]控股$/.test(compactName)) {
    return "腾讯控股";
  }

  return cleanedName;
}

module.exports = {
  inferMarket,
  toApiSymbol,
  normalizeMarket,
  cleanRawSymbol,
  normalizeSecurityName
};
