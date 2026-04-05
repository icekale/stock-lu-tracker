const SYMBOL_NAME_OVERRIDES = Object.freeze({
  "000776": "广发证券",
  "000807": "云铝股份",
  "001286": "陕西能源",
  "00700": "腾讯控股",
  "00883": "中国海洋石油",
  "00902": "华能国际电力股份",
  "00941": "中国移动",
  "01030": "新城发展",
  "01088": "中国神华",
  "01898": "中煤能源",
  "03933": "联邦制药",
  "511880": "银华日利",
  "600863": "内蒙华电",
  "600900": "长江电力",
  "600941": "中国移动",
  "600985": "淮北矿业",
  "601088": "中国神华",
  "601225": "陕西煤业",
  "601899": "紫金矿业"
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

function normalizeSecurityName(rawSymbol, rawName, marketInput, options = {}) {
  const market = normalizeMarket(rawSymbol, marketInput);
  const normalizedSymbol = cleanRawSymbol(toApiSymbol(rawSymbol, market) || rawSymbol);
  const nameSource = String(options?.nameSource || "")
    .trim()
    .toLowerCase();
  const cleanedName = cleanSecurityName(rawName);
  const compactName = cleanedName.replace(/\s+/g, "");

  if (nameSource === "xueqiu" && cleanedName) {
    return cleanedName;
  }

  if (/^时(?:暑)?讯控股$/.test(compactName) || /^腾[讯汛迅]控股$/.test(compactName)) {
    return "腾讯控股";
  }

  if (normalizedSymbol && SYMBOL_NAME_OVERRIDES[normalizedSymbol]) {
    return SYMBOL_NAME_OVERRIDES[normalizedSymbol];
  }

  return cleanedName;
}

module.exports = {
  SYMBOL_NAME_OVERRIDES,
  inferMarket,
  toApiSymbol,
  normalizeMarket,
  cleanRawSymbol,
  normalizeSecurityName
};
