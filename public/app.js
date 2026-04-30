const state = {
  snapshots: [],
  selectedSnapshotId: null,
  currentTab: "adjust",
  monthlyChart: null,
  detailExpandedId: null,
  monthNavFeedbackSnapshotId: null
};

const els = {
  tabLatestBtn: document.getElementById("tabLatestBtn"),
  tabAdjustBtn: document.getElementById("tabAdjustBtn"),
  monthPrevBtn: document.getElementById("monthPrevBtn"),
  monthNextBtn: document.getElementById("monthNextBtn"),
  monthYearGroup: document.getElementById("monthYearGroup"),
  monthSegmentGroup: document.getElementById("monthSegmentGroup"),
  snapshotSelect: document.getElementById("snapshotSelect"),
  adjustView: document.getElementById("adjustView"),
  latestView: document.getElementById("latestView"),
  adjustRowsBody: document.getElementById("adjustRowsBody"),
  openRowsBody: document.getElementById("openRowsBody"),
  closeRowsBody: document.getElementById("closeRowsBody"),
  latestRowsBody: document.getElementById("latestRowsBody"),
  totalMarketValue: document.getElementById("totalMarketValue"),
  monthlyReturnValue: document.getElementById("monthlyReturnValue"),
  monthlyReturnMeta: document.getElementById("monthlyReturnMeta"),
  cumulativeNetValue: document.getElementById("cumulativeNetValue"),
  netIndexValue: document.getElementById("netIndexValue"),
  yearStartIndexValue: document.getElementById("yearStartIndexValue"),
  holdingCountMeta: document.getElementById("holdingCountMeta"),
  outperformValue: document.getElementById("outperformValue"),
  outperformMeta: document.getElementById("outperformMeta"),
  latestMonthValue: document.getElementById("latestMonthValue"),
  latestUpdateMeta: document.getElementById("latestUpdateMeta"),
  selectedSnapshotMeta: document.getElementById("selectedSnapshotMeta"),
  detailListMeta: document.getElementById("detailListMeta"),
  detailSnapshotList: document.getElementById("detailSnapshotList"),
  monthlyNetworthChart: document.getElementById("monthlyNetworthChart"),
  monthlyChartEmpty: document.getElementById("monthlyChartEmpty"),
  chartSeriesCount: document.getElementById("chartSeriesCount"),
  chartLatestMonth: document.getElementById("chartLatestMonth"),
  chartLatestValue: document.getElementById("chartLatestValue"),
  chartSeriesHint: document.getElementById("chartSeriesHint")
};

async function request(url, options = {}) {
  let response;
  try {
    response = await fetch(url, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {})
      }
    });
  } catch {
    throw new Error("无法连接本地服务，请确认 `npm start` 正在运行");
  }

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || `请求失败 (${response.status})`);
  }
  return data;
}

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

function formatNumber(value, digits = 0) {
  const number = toNumber(value);
  if (number === null) {
    return "-";
  }

  return number.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatCost(value) {
  return formatCurrency(value, 2);
}

function formatCurrency(value, digits = 0) {
  const number = toNumber(value);
  if (number === null) {
    return "-";
  }
  return `¥ ${formatNumber(number, digits)}`;
}

function formatIndex(value) {
  const number = toNumber(value);
  if (number === null || number <= 0) {
    return "-";
  }
  return number.toFixed(4);
}

function formatPercent(value, digits = 2) {
  const number = toNumber(value);
  if (number === null) {
    return "-";
  }
  const prefix = number > 0 ? "+" : "";
  return `${prefix}${number.toFixed(digits)}%`;
}

function formatDateTime(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return "-";
  }

  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  });
}

function formatMonthDay(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return "-";
  }

  return `${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function sourceLabel(value) {
  const source = String(value || "").trim().toLowerCase();
  if (source === "xueqiu") {
    return "雪球";
  }
  if (source === "weibo") {
    return "微博";
  }
  if (!source) {
    return "-";
  }
  return source;
}

function formatDelta(value) {
  const number = toNumber(value);
  if (number === null) {
    return "0";
  }
  if (number > 0) {
    return `+${formatNumber(number, 0)}`;
  }
  if (number < 0) {
    return `-${formatNumber(Math.abs(number), 0)}`;
  }
  return "0";
}

function deltaClass(value) {
  const number = toNumber(value) || 0;
  if (number > 0) {
    return "delta-pos";
  }
  if (number < 0) {
    return "delta-neg";
  }
  return "delta-zero";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeBadgeClass(value) {
  return ["buy", "sell", "hold", "new"].includes(value) ? value : "hold";
}

function normalizeSymbol(rawSymbol) {
  const value = String(rawSymbol || "").trim().toUpperCase();
  if (!value) {
    return "-";
  }

  if (/^\d{6}\.(SH|SZ)$/.test(value) || /^\d{4,5}\.HK$/.test(value)) {
    return value;
  }

  if (/^\d{6}$/.test(value)) {
    const suffix = /^[569]/.test(value) ? "SH" : "SZ";
    return `${value}.${suffix}`;
  }

  if (/^\d{4,5}$/.test(value)) {
    return `${value}.HK`;
  }

  return value;
}

function getMarketBucket(rawSymbol) {
  const symbol = normalizeSymbol(rawSymbol);
  if (/^\d{6}\.(SH|SZ|SS)$/.test(symbol)) {
    return "a";
  }
  if (/^\d{4,5}\.HK$/.test(symbol)) {
    return "h";
  }
  return "other";
}

function groupRowsByMarket(rows) {
  const groups = [
    { key: "a", label: "A股", rows: [] },
    { key: "h", label: "H股", rows: [] },
    { key: "other", label: "其他", rows: [] }
  ];

  const groupMap = new Map(groups.map((item) => [item.key, item]));
  rows.forEach((item) => {
    const bucket = getMarketBucket(item.symbol);
    const group = groupMap.get(bucket) || groupMap.get("other");
    group.rows.push(item);
  });

  return groups.filter((item) => item.rows.length > 0);
}

function renderGroupedRows(rows, columnCount, renderRow) {
  return groupRowsByMarket(rows)
    .map((group) => {
      const label = escapeHtml(group.label);
      const count = escapeHtml(`${group.rows.length}只`);
      const groupKey = escapeHtml(group.key);
      return `
        <tr class="market-group-row" data-market-group="${groupKey}">
          <td colspan="${columnCount}">
            <div class="market-group-cell">
              <span class="market-group-label" data-market-group-label="${groupKey}">${label}</span>
              <span class="market-group-count">${count}</span>
            </div>
          </td>
        </tr>
        ${group.rows.map((item) => renderRow(item)).join("")}
      `;
    })
    .join("");
}

function monthLabel(snapshot) {
  const title = String(snapshot?.title || "");
  const matched = title.match(/(20\d{2})\s*年\s*(\d{1,2})\s*月/);
  if (matched) {
    return `${matched[1]}-${String(Number(matched[2])).padStart(2, "0")}`;
  }

  const date = snapshot?.postedAt ? new Date(snapshot.postedAt) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return "-";
  }
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function getSnapshotMonthMeta(snapshot) {
  const month = monthLabel(snapshot);
  if (!month || month === "-" || !month.includes("-")) {
    return {
      month: "-",
      year: "-",
      monthNumber: "-"
    };
  }

  const [year, monthNumber] = month.split("-");
  return {
    month,
    year,
    monthNumber
  };
}

function getRowQty(row) {
  const candidates = [row.holdingQty, row.availableQty, row.balanceQty, row.changeQty]
    .map((item) => toNumber(item))
    .filter((item) => item !== null && item > 0);

  if (candidates.length === 0) {
    return 0;
  }

  return Math.max(...candidates);
}

function toRowModel(row) {
  const qty = getRowQty(row);
  const cost = toNumber(row.referenceCost) ?? toNumber(row.latestCost);
  const price = toNumber(row.latestPrice);
  let marketValue = toNumber(row.marketValue);
  const floatingPnl = toNumber(row.floatingPnl);
  const expected = qty > 0 && price !== null ? qty * price : null;

  if ((marketValue === null || marketValue <= 0) && expected !== null && expected > 0) {
    marketValue = expected;
  }

  if (marketValue !== null && marketValue < 100 && expected !== null && expected > 10_000) {
    marketValue = expected;
  }

  let qualityScore = 10;
  if (expected !== null && expected > 0 && marketValue !== null && marketValue > 0) {
    qualityScore = Math.abs(Math.log(marketValue / expected));
  }

  if (qty > 50_000_000) {
    qualityScore += 6;
  }
  if (price !== null && price > 10_000) {
    qualityScore += 6;
  }
  if (marketValue !== null && marketValue > 300_000_000) {
    qualityScore += 4;
  }

  return {
    symbol: normalizeSymbol(row.symbol),
    name: String(row.name || "-").trim() || "-",
    qty,
    cost,
    price,
    marketValue,
    floatingPnl,
    qualityScore
  };
}

function currentSnapshot() {
  if (!state.selectedSnapshotId) {
    return state.snapshots[0] || null;
  }
  return state.snapshots.find((item) => item.id === state.selectedSnapshotId) || state.snapshots[0] || null;
}

function currentSnapshotIndex() {
  const current = currentSnapshot();
  if (!current) {
    return -1;
  }

  return state.snapshots.findIndex((item) => item.id === current.id);
}

function olderSnapshot() {
  const index = currentSnapshotIndex();
  if (index < 0 || index >= state.snapshots.length - 1) {
    return null;
  }

  return state.snapshots[index + 1] || null;
}

function newerSnapshot() {
  const index = currentSnapshotIndex();
  if (index <= 0) {
    return null;
  }

  return state.snapshots[index - 1] || null;
}

function resolveYearSwitchSnapshot(targetYear) {
  const year = String(targetYear || "").trim();
  if (!year) {
    return null;
  }

  const candidates = state.snapshots
    .map((snapshot) => ({
      snapshot,
      meta: getSnapshotMonthMeta(snapshot)
    }))
    .filter((item) => item.meta.year === year && item.meta.monthNumber !== "-");

  if (candidates.length === 0) {
    return null;
  }

  const currentMeta = getSnapshotMonthMeta(currentSnapshot());
  const currentMonthNumber = Number(currentMeta.monthNumber);
  if (!Number.isFinite(currentMonthNumber)) {
    return candidates[0].snapshot;
  }

  const exactMatch = candidates.find((item) => Number(item.meta.monthNumber) === currentMonthNumber);
  if (exactMatch) {
    return exactMatch.snapshot;
  }

  const closest = candidates.reduce((best, item) => {
    const monthNumber = Number(item.meta.monthNumber);
    if (!Number.isFinite(monthNumber)) {
      return best;
    }

    const candidate = {
      snapshot: item.snapshot,
      monthNumber,
      distance: Math.abs(monthNumber - currentMonthNumber)
    };

    if (!best) {
      return candidate;
    }

    if (candidate.distance < best.distance) {
      return candidate;
    }

    if (candidate.distance === best.distance && candidate.monthNumber > best.monthNumber) {
      return candidate;
    }

    return best;
  }, null);

  return closest?.snapshot || candidates[0].snapshot;
}

function updateMonthNavButton(button, snapshot, fallbackLabel, hintLabel = fallbackLabel) {
  if (!button) {
    return;
  }

  const label = snapshot ? `切换到 ${monthLabel(snapshot)}` : fallbackLabel;
  const isDisabled = !snapshot?.id;
  button.disabled = isDisabled;
  button.title = label;
  button.setAttribute("aria-label", label);
  button.dataset.navHint = snapshot ? hintLabel : fallbackLabel;
}

function prefersReducedMotion() {
  return Boolean(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
}

function centerElementInScroller(container, element, behavior = "smooth") {
  if (!(container instanceof HTMLElement) || !(element instanceof HTMLElement)) {
    return;
  }

  const maxScrollLeft = container.scrollWidth - container.clientWidth;
  if (maxScrollLeft <= 4) {
    return;
  }

  const targetLeft = element.offsetLeft - (container.clientWidth - element.offsetWidth) / 2;
  const nextLeft = Math.max(0, Math.min(maxScrollLeft, targetLeft));
  container.scrollTo({
    left: nextLeft,
    behavior
  });
}

function syncMonthNavigatorViewport(shouldAnimate = false) {
  const behavior = shouldAnimate && !prefersReducedMotion() ? "smooth" : "auto";

  window.requestAnimationFrame(() => {
    const activeFolder = els.monthYearGroup?.querySelector(".month-folder.is-active");
    if (activeFolder instanceof HTMLElement) {
      activeFolder.scrollIntoView({
        block: "nearest",
        inline: "nearest",
        behavior
      });
    }

    const activeMonth = els.monthYearGroup?.querySelector(".month-folder.is-active .month-chip-btn.is-active");
    const activeMonthStrip = activeMonth?.closest(".month-folder-months");
    centerElementInScroller(activeMonthStrip, activeMonth, behavior);
  });
}

function isTextInputLike(target) {
  if (!(target instanceof HTMLElement)) {
    return false;
  }

  if (target.isContentEditable) {
    return true;
  }

  return Boolean(target.closest("input, textarea, select, button, a, summary, [role='button'], [role='tab']"));
}

function previousSnapshot() {
  const current = currentSnapshot();
  if (!current) {
    return null;
  }

  const index = state.snapshots.findIndex((item) => item.id === current.id);
  if (index < 0 || index === state.snapshots.length - 1) {
    return null;
  }

  return state.snapshots[index + 1];
}

function buildRowsFromSnapshot(snapshot) {
  if (!snapshot || !Array.isArray(snapshot.rows)) {
    return [];
  }

  const rowsBySymbol = new Map();

  for (const row of snapshot.rows) {
    const item = toRowModel(row);
    if (item.qty <= 0 || item.symbol === "-") {
      continue;
    }

    const existing = rowsBySymbol.get(item.symbol);
    if (!existing) {
      rowsBySymbol.set(item.symbol, item);
      continue;
    }

    const shouldReplace =
      item.qualityScore < existing.qualityScore ||
      (item.qualityScore === existing.qualityScore &&
        (toNumber(item.marketValue) || 0) > (toNumber(existing.marketValue) || 0));

    if (shouldReplace) {
      rowsBySymbol.set(item.symbol, item);
    }
  }

  return [...rowsBySymbol.values()].sort((a, b) => {
    const aValue = toNumber(a.marketValue) || 0;
    const bValue = toNumber(b.marketValue) || 0;
    return bValue - aValue;
  });
}

function buildCurrentRows() {
  return buildRowsFromSnapshot(currentSnapshot());
}

function buildPrevMap(previousRows = buildRowsFromSnapshot(previousSnapshot())) {
  const map = new Map();

  for (const item of previousRows) {
    map.set(item.symbol, item);
  }

  return map;
}

function buildAdjustRows(currentRows = buildCurrentRows(), prevMap = buildPrevMap()) {
  return currentRows.map((item) => {
    const prev = prevMap.get(item.symbol);
    const prevQty = prev ? prev.qty : 0;
    const delta = item.qty - prevQty;

    let actionLabel = "持仓不变";
    let actionClass = "hold";

    if (prevQty <= 0 && item.qty > 0) {
      actionLabel = "新进";
      actionClass = "new";
    } else if (delta > 0) {
      actionLabel = "加仓";
      actionClass = "buy";
    } else if (delta < 0) {
      actionLabel = "减仓";
      actionClass = "sell";
    }

    return {
      ...item,
      delta,
      actionLabel,
      actionClass
    };
  });
}

function buildOpenRows(adjustRows) {
  return adjustRows.filter((item) => item.actionLabel === "新进");
}

function buildCloseRows(currentRows = buildCurrentRows(), prevMap = buildPrevMap()) {
  const currentSet = new Set(currentRows.map((item) => item.symbol));
  const rows = [];

  for (const item of prevMap.values()) {
    if (!currentSet.has(item.symbol) && item.qty > 0) {
      rows.push(item);
    }
  }

  return rows;
}

function sumMarketValuesFromRows(rows) {
  return rows
    .map((item) => toNumber(item.marketValue))
    .filter((value) => Number.isFinite(value) && value > 0)
    .reduce((sum, value) => sum + value, 0);
}

function normalizeStatsText(text) {
  return String(text || "")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function truncateText(value, max = 220) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }
  return text.length <= max ? text : `${text.slice(0, max)}...`;
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

function toSignedPercentByDirection(directionText, valueText) {
  const value = toNumber(valueText);
  if (value === null) {
    return null;
  }

  const direction = String(directionText || "").trim();
  if (/下跌|亏损|跑输/.test(direction)) {
    return -Math.abs(value);
  }
  if (/上涨|盈利|跑赢/.test(direction)) {
    return Math.abs(value);
  }
  return value;
}

function toSignedYuanByDirection(directionText, valueText, unitText) {
  const amount = toYuanByUnit(valueText, unitText, String(directionText || ""));
  if (amount === null) {
    return null;
  }

  const direction = String(directionText || "").trim();
  if (/亏损/.test(direction)) {
    return -Math.abs(amount);
  }
  return amount;
}

function parseMonthlyPerformance(snapshot) {
  const mergedText = normalizeStatsText(`${String(snapshot?.rawText || "")}\n${String(snapshot?.ocrText || "")}`);
  if (!mergedText) {
    return {
      profitAmount: null,
      profitPct: null,
      benchmarkPct: null,
      outperformPct: null
    };
  }

  let profitAmount = null;
  let profitPct = null;
  let benchmarkPct = null;
  let outperformPct = null;

  const profitAmountMatch = mergedText.match(/本月(盈利|亏损)\s*([0-9]+(?:\.[0-9]+)?)\s*([Ww万亿]?)/);
  if (profitAmountMatch) {
    profitAmount = toSignedYuanByDirection(profitAmountMatch[1], profitAmountMatch[2], profitAmountMatch[3]);
    if (profitAmount !== null && !profitAmountMatch[3] && Math.abs(profitAmount) < 100_000) {
      profitAmount *= 10_000;
    }
  }

  const profitPctMatch = mergedText.match(/本月(盈利|亏损)\s*([0-9]+(?:\.[0-9]+)?)%/);
  if (profitPctMatch) {
    profitPct = toSignedPercentByDirection(profitPctMatch[1], profitPctMatch[2]);
  }

  const benchmarkMatch = mergedText.match(/(?:本月)?(?:上证指数|沪指|上证)\s*(上涨|下跌)?\s*([0-9]+(?:\.[0-9]+)?)%/);
  if (benchmarkMatch) {
    benchmarkPct = toSignedPercentByDirection(benchmarkMatch[1], benchmarkMatch[2]);
  }

  const outperformMatch = mergedText.match(/本月跑(赢|输)\s*([0-9]+(?:\.[0-9]+)?)%/);
  if (outperformMatch) {
    outperformPct = toSignedPercentByDirection(`跑${outperformMatch[1]}`, outperformMatch[2]);
  }

  return {
    profitAmount,
    profitPct,
    benchmarkPct,
    outperformPct
  };
}

function parsePostMetrics(snapshot) {
  if (!snapshot) {
    return {
      cumulativeNetValue: null,
      netIndex: null,
      yearStartNetIndex: null
    };
  }

  const rawText = normalizeStatsText(snapshot.rawText);
  const mergedText = normalizeStatsText(`${String(snapshot.rawText || "")}\n${String(snapshot.ocrText || "")}`);
  const primaryText = rawText || mergedText;

  const explicitCumulativeNetValue = toNumber(snapshot.cumulativeNetValue);
  const explicitNetIndex = toNumber(snapshot.netIndex);
  const explicitYearStartNetIndex = toNumber(snapshot.yearStartNetIndex);

  const cumulativeNetValue =
    explicitCumulativeNetValue ?? parseCumulativeNetValue(primaryText) ?? parseCumulativeNetValue(mergedText);
  const netIndex = explicitNetIndex ?? parseNetIndex(primaryText) ?? parseNetIndex(mergedText);
  const yearStartNetIndex =
    explicitYearStartNetIndex ?? parseYearStartIndex(primaryText) ?? parseYearStartIndex(mergedText);

  return {
    cumulativeNetValue,
    netIndex,
    yearStartNetIndex
  };
}

function resolveSnapshotPortfolioMarketValue(snapshot, rows = null) {
  const refValue = parseReferenceStockValue(snapshot);
  if (refValue !== null) {
    return refValue;
  }

  if (Array.isArray(rows)) {
    return sumMarketValuesFromRows(rows);
  }

  return snapshotMarketValueFromRows(snapshot);
}

function resolveSnapshotChartValue(snapshot, rows = null, postMetrics = parsePostMetrics(snapshot)) {
  if (postMetrics.cumulativeNetValue !== null) {
    return postMetrics.cumulativeNetValue;
  }

  return resolveSnapshotPortfolioMarketValue(snapshot, rows);
}

function buildRenderPayload() {
  const current = currentSnapshot();
  const previous = previousSnapshot();
  const currentRows = buildCurrentRows();
  const previousRows = buildRowsFromSnapshot(previous);
  const prevMap = buildPrevMap(previousRows);
  const adjustRows = buildAdjustRows(currentRows, prevMap);
  const openRows = buildOpenRows(adjustRows);
  const closeRows = buildCloseRows(currentRows, prevMap);
  const currentPostMetrics = parsePostMetrics(current);
  const previousPostMetrics = parsePostMetrics(previous);
  const currentMarketValue = resolveSnapshotPortfolioMarketValue(current, currentRows);
  const previousMarketValue = resolveSnapshotPortfolioMarketValue(previous, previousRows);

  return {
    current,
    previous,
    currentRows,
    adjustRows,
    openRows,
    closeRows,
    currentMarketValue,
    previousMarketValue,
    currentPostMetrics,
    previousPostMetrics,
    holdingCount: currentRows.length
  };
}

function getRowReviewLevel(item) {
  if (!item) {
    return "none";
  }

  if (item.cost === null || item.marketValue === null) {
    return "strong";
  }

  if (item.qualityScore >= 1.75) {
    return "strong";
  }

  if (item.qualityScore >= 1.15) {
    return "mild";
  }

  return "none";
}

function needsRowReview(item) {
  return getRowReviewLevel(item) !== "none";
}

function buildPerformanceSummary(payload) {
  const currentValue = toNumber(payload.currentMarketValue);
  const previousValue = toNumber(payload.previousMarketValue);
  const parsed = parseMonthlyPerformance(payload.current);

  const fallbackProfitAmount =
    currentValue !== null && previousValue !== null && previousValue > 0 ? currentValue - previousValue : null;
  const fallbackProfitPct =
    fallbackProfitAmount !== null && previousValue !== null && previousValue > 0
      ? (fallbackProfitAmount / previousValue) * 100
      : null;

  const profitAmount = parsed.profitAmount ?? fallbackProfitAmount;
  const profitPct = parsed.profitPct ?? fallbackProfitPct;
  const benchmarkPct = parsed.benchmarkPct;
  const outperformPct =
    parsed.outperformPct ?? (profitPct !== null && benchmarkPct !== null ? profitPct - benchmarkPct : null);

  return {
    profitAmount,
    profitPct,
    benchmarkPct,
    outperformPct
  };
}

function buildMonthNavigatorModel() {
  const groups = new Map();

  for (const snapshot of state.snapshots) {
    const meta = getSnapshotMonthMeta(snapshot);
    if (meta.month === "-") {
      continue;
    }

    if (!groups.has(meta.year)) {
      groups.set(meta.year, []);
    }

    groups.get(meta.year).push({
      id: snapshot.id,
      month: meta.month,
      monthNumber: meta.monthNumber,
      postedAt: snapshot.postedAt
    });
  }

  const selected = currentSnapshot();
  const selectedMeta = getSnapshotMonthMeta(selected);
  const years = [...groups.keys()].sort((a, b) => Number(b) - Number(a));
  const selectedYear = selectedMeta.year !== "-" ? selectedMeta.year : years[0] || "";
  const selectedIndex = currentSnapshotIndex();

  return {
    groups,
    years,
    selectedYear,
    selectedSnapshotId: selected?.id || null,
    selectedIndex,
    olderSnapshot: olderSnapshot(),
    newerSnapshot: newerSnapshot()
  };
}

function summarizeNameValidation(rows) {
  const summary = {
    xueqiu: 0,
    tencent: 0,
    manual: 0,
    unknown: 0
  };

  for (const row of Array.isArray(rows) ? rows : []) {
    const source = String(row?.nameSource || "")
      .trim()
      .toLowerCase();

    if (source === "xueqiu") {
      summary.xueqiu += 1;
    } else if (source === "tencent") {
      summary.tencent += 1;
    } else if (source === "manual") {
      summary.manual += 1;
    } else {
      summary.unknown += 1;
    }
  }

  return summary;
}

function formatNameValidationSummary(summary) {
  const parts = [];
  if (summary.xueqiu > 0) {
    parts.push(`雪球 ${summary.xueqiu}`);
  }
  if (summary.tencent > 0) {
    parts.push(`腾讯 ${summary.tencent}`);
  }
  if (summary.manual > 0) {
    parts.push(`人工 ${summary.manual}`);
  }
  if (summary.unknown > 0) {
    parts.push(`待确认 ${summary.unknown}`);
  }
  return parts.length > 0 ? parts.join(" / ") : "未记录";
}

function summarizeSnapshotReview(rows) {
  let needsReview = 0;

  for (const row of Array.isArray(rows) ? rows : []) {
    const item = toRowModel(row);
    if (item.qty <= 0 || needsRowReview(item)) {
      needsReview += 1;
    }
  }

  return {
    needsReview
  };
}

function buildMarketSummaryText(rows) {
  const groups = groupRowsByMarket(rows).map((group) => `${group.label} ${group.rows.length}只`);
  return groups.length > 0 ? groups.join(" / ") : "暂无分组";
}

function buildSnapshotSummaryHtml(snapshot, rows, postMetrics) {
  const review = summarizeSnapshotReview(snapshot?.rows || []);
  const validation = summarizeNameValidation(snapshot?.rows || []);
  const marketSummary = buildMarketSummaryText(rows);
  const snippet = truncateText(normalizeStatsText(snapshot?.rawText || snapshot?.ocrText || ""), 220);
  const summaryLines = [
    {
      label: "月份",
      value: monthLabel(snapshot)
    },
    {
      label: "发帖时间",
      value: formatDateTime(snapshot?.postedAt)
    },
    {
      label: "净值锚点",
      value: formatCurrency(postMetrics?.cumulativeNetValue, 0)
    },
    {
      label: "净值指数",
      value: formatIndex(postMetrics?.netIndex)
    },
    {
      label: "年初净值指数",
      value: formatIndex(postMetrics?.yearStartNetIndex)
    },
    {
      label: "市场分布",
      value: marketSummary
    },
    {
      label: "名称校验",
      value: formatNameValidationSummary(validation)
    },
    {
      label: "复核提示",
      value:
        review.needsReview > 0
          ? `${review.needsReview} 行建议复核`
          : "当前结构化结果整体正常"
    }
  ];

  return `
    <div class="detail-summary-copy">
      <p class="detail-lead">
        ${escapeHtml(
          `${monthLabel(snapshot)} 快照包含 ${rows.length} 只持仓，来源 ${sourceLabel(snapshot?.source)}，自动导入 ${Number(
            snapshot?.importedTrades
          ) || 0} 笔交易。`
        )}
      </p>
      <div class="detail-summary-list">
        ${summaryLines
          .map(
            (item) => `
              <div class="detail-summary-line">
                <span>${escapeHtml(item.label)}</span>
                <strong>${escapeHtml(item.value)}</strong>
              </div>
            `
          )
          .join("")}
      </div>
      <div class="detail-chip-row">
        <span class="detail-chip">原图 ${escapeHtml(String(Array.isArray(snapshot?.images) ? snapshot.images.length : 0))} 张</span>
        <span class="detail-chip">OCR ${snapshot?.ocrText ? "已识别" : "未触发"}</span>
        <span class="detail-chip">${escapeHtml(`帖子ID ${String(snapshot?.postId || "-").replace(/^xq:|^wb:/i, "")}`)}</span>
      </div>
      <p class="detail-snippet">
        ${snippet ? escapeHtml(`正文摘录：${snippet}`) : "当前月份未保存可用的原帖正文摘录。"}
      </p>
    </div>
  `;
}

function buildDetailImageGridHtml(snapshot) {
  const images = Array.isArray(snapshot?.images) ? snapshot.images : [];
  if (images.length === 0) {
    return `<div class="detail-empty">暂无原图</div>`;
  }

  return images
    .map(
      (url, index) => `
        <a
          class="detail-image-card"
          href="${escapeHtml(url)}"
          target="_blank"
          rel="noreferrer"
        >
          <img src="${escapeHtml(url)}" alt="${escapeHtml(`持仓图 ${index + 1}`)}" loading="lazy" />
          <span class="detail-image-footer">
            <span>持仓图 ${escapeHtml(String(index + 1))}</span>
            <span>Open</span>
          </span>
        </a>
      `
    )
    .join("");
}

function buildDetailInsightGridHtml(snapshot, rows) {
  const review = summarizeSnapshotReview(snapshot?.rows || []);
  const validation = summarizeNameValidation(snapshot?.rows || []);
  const metrics = [
    {
      label: "数据来源",
      value: sourceLabel(snapshot?.source)
    },
    {
      label: "结构化持仓",
      value: `${rows.length} 只`
    },
    {
      label: "原图数量",
      value: `${Array.isArray(snapshot?.images) ? snapshot.images.length : 0} 张`
    },
    {
      label: "自动导入交易",
      value: `${Number(snapshot?.importedTrades) || 0} 笔`
    },
    {
      label: "名称校验",
      value: formatNameValidationSummary(validation)
    },
    {
      label: "待复核",
      value: review.needsReview > 0 ? `${review.needsReview} 行` : "通过",
      valueClass: review.needsReview > 0 ? "review-alert" : "review-ok"
    }
  ];

  return metrics
    .map(
      (item) => `
        <article class="detail-insight">
          <p class="detail-insight-label">${escapeHtml(item.label)}</p>
          <p class="detail-insight-value ${escapeHtml(item.valueClass || "")}">${escapeHtml(item.value)}</p>
        </article>
      `
    )
    .join("");
}

function buildDetailTextPanelHtml(snapshot, rows, postMetrics) {
  const rawText = String(snapshot?.rawText || "").trim() || "暂无原帖正文";
  const ocrText = String(snapshot?.ocrText || "").trim() || "暂无 OCR 文本";
  const title = escapeHtml(snapshot?.title || `${monthLabel(snapshot)} 月度详情`);
  const postedAt = escapeHtml(formatDateTime(snapshot?.postedAt));
  const postLink = snapshot?.link
    ? `<a class="detail-link-btn" href="${escapeHtml(snapshot.link)}" target="_blank" rel="noreferrer">查看原帖</a>`
    : "";

  return `
    <section class="detail-panel">
      <div class="detail-panel-head">
        <div>
          <h4>${title}</h4>
          <p>${escapeHtml(`${postedAt} · ${sourceLabel(snapshot?.source)}`)}</p>
        </div>
        ${postLink}
      </div>

      <div class="detail-summary-panel">
        ${buildSnapshotSummaryHtml(snapshot, rows, postMetrics)}
      </div>

      <div class="detail-text-stack">
        <section class="detail-text-block">
          <div class="detail-text-head">
            <h5>原帖正文</h5>
          </div>
          <pre class="detail-text-pre">${escapeHtml(rawText)}</pre>
        </section>
        <section class="detail-text-block">
          <div class="detail-text-head">
            <h5>OCR 文本</h5>
          </div>
          <pre class="detail-text-pre">${escapeHtml(ocrText)}</pre>
        </section>
      </div>
    </section>
  `;
}

function buildSnapshotDetailItemHtml(snapshot, isOpen) {
  const rows = buildRowsFromSnapshot(snapshot);
  const postMetrics = parsePostMetrics(snapshot);
  const review = summarizeSnapshotReview(snapshot?.rows || []);
  const itemId = String(snapshot?.id || "");
  const month = monthLabel(snapshot);
  const shortDate = formatMonthDay(snapshot?.postedAt);
  const bodyId = `detail-item-body-${itemId}`;
  const metaParts = [
    sourceLabel(snapshot?.source),
    `${rows.length} 只持仓`,
    `${Array.isArray(snapshot?.images) ? snapshot.images.length : 0} 张原图`,
    review.needsReview > 0 ? `待复核 ${review.needsReview} 行` : "结构化正常"
  ];

  return `
    <article class="detail-item ${isOpen ? "is-open" : ""}">
      <button
        type="button"
        class="detail-item-toggle"
        data-detail-snapshot="${escapeHtml(itemId)}"
        aria-expanded="${isOpen ? "true" : "false"}"
        aria-controls="${escapeHtml(bodyId)}"
      >
        <span class="detail-item-copy">
          <span class="detail-item-date-wrap">
            <strong class="detail-item-month">${escapeHtml(month)}</strong>
            <span class="detail-item-date">${escapeHtml(shortDate)}</span>
          </span>
          <span class="detail-item-meta">${escapeHtml(metaParts.join(" · "))}</span>
        </span>
        <span class="detail-item-chevron" aria-hidden="true">›</span>
      </button>
      <div id="${escapeHtml(bodyId)}" class="detail-item-body ${isOpen ? "" : "hidden"}">
        <section class="detail-insight-grid">
          ${buildDetailInsightGridHtml(snapshot, rows)}
        </section>
        <div class="detail-content-grid">
          <section class="detail-panel">
            <div class="detail-panel-head">
              <div>
                <h4>持仓原图</h4>
                <p>${escapeHtml(
                  Array.isArray(snapshot?.images) && snapshot.images.length > 0
                    ? `${snapshot.images.length} 张截图 · 点击查看原图`
                    : "当前月份没有保存原图"
                )}</p>
              </div>
            </div>
            <div class="detail-image-grid">
              ${buildDetailImageGridHtml(snapshot)}
            </div>
          </section>
          ${buildDetailTextPanelHtml(snapshot, rows, postMetrics)}
        </div>
      </div>
    </article>
  `;
}

function renderSnapshotDetailList() {
  if (!els.detailSnapshotList || !els.detailListMeta) {
    return;
  }

  if (!Array.isArray(state.snapshots) || state.snapshots.length === 0) {
    els.detailListMeta.textContent = "暂无可展开的月份详情";
    els.detailSnapshotList.innerHTML = `<div class="detail-empty">暂无月度详情</div>`;
    return;
  }

  const selected = currentSnapshot();
  els.detailListMeta.textContent = selected
    ? `当前顶部视图：${monthLabel(selected)} · 共 ${state.snapshots.length} 个月份可展开查看`
    : `共 ${state.snapshots.length} 个月份可展开查看`;

  els.detailSnapshotList.innerHTML = state.snapshots
    .map((snapshot) => buildSnapshotDetailItemHtml(snapshot, state.detailExpandedId === snapshot.id))
    .join("");
}

function renderMonthSelect() {
  const hasNativeSelect = Boolean(els.snapshotSelect);
  const hasNavigator = Boolean(els.monthYearGroup && els.monthSegmentGroup);

  if (!hasNativeSelect && !hasNavigator) {
    return;
  }

  if (!state.selectedSnapshotId && state.snapshots[0]?.id) {
    state.selectedSnapshotId = state.snapshots[0].id;
  }

  if (hasNativeSelect) {
    const options = state.snapshots.map((snapshot) => {
      const label = monthLabel(snapshot);
      return `<option value="${escapeHtml(snapshot.id)}">${escapeHtml(label)}</option>`;
    });

    els.snapshotSelect.innerHTML = options.join("");
    els.snapshotSelect.value = state.selectedSnapshotId || "";
  }

  if (!hasNavigator) {
    return;
  }

  const navigatorModel = buildMonthNavigatorModel();
  const shouldPulseSelection = state.monthNavFeedbackSnapshotId === navigatorModel.selectedSnapshotId;
  const yearFolders = navigatorModel.years.map((year) => {
    const items = navigatorModel.groups.get(year) || [];
    const isActive = navigatorModel.selectedYear === year;
    const targetSnapshot = resolveYearSwitchSnapshot(year);
    const targetLabel = targetSnapshot ? monthLabel(targetSnapshot) : `${year}-01`;
    const activeItem = items.find((item) => item.id === navigatorModel.selectedSnapshotId) || items[0] || null;
    const folderMeta = isActive
      ? `当前 ${activeItem?.monthNumber || "--"} 月 · 共 ${items.length} 个月`
      : `收录 ${items.length} 个月 · 最近 ${items[0]?.monthNumber || "--"} 月`;
    const monthButtons = items.map((item) => {
      const isMonthActive = navigatorModel.selectedSnapshotId === item.id;
      return `
        <button
          type="button"
          class="month-chip-btn ${isMonthActive ? "is-active" : ""} ${
            isMonthActive && shouldPulseSelection ? "is-just-selected" : ""
          }"
          data-select-snapshot="${escapeHtml(item.id)}"
          aria-pressed="${isMonthActive ? "true" : "false"}"
          aria-current="${isMonthActive ? "date" : "false"}"
          title="切换到 ${escapeHtml(item.month)}"
          aria-label="切换到 ${escapeHtml(item.month)}"
        >
          <span>${escapeHtml(item.monthNumber)}</span>
          <small>月</small>
        </button>
      `;
    });

    const panelId = `month-folder-panel-${escapeHtml(year)}`;
    return `
      <div class="month-folder ${isActive ? "is-active" : ""}" data-year-folder="${escapeHtml(year)}">
        <button
          type="button"
          class="month-folder-toggle ${isActive ? "is-active" : ""}"
          data-select-year="${escapeHtml(year)}"
          data-select-year-snapshot="${escapeHtml(targetSnapshot?.id || "")}"
          aria-expanded="${isActive ? "true" : "false"}"
          aria-controls="${panelId}"
          title="切换到 ${escapeHtml(targetLabel)}"
          aria-label="切换到 ${escapeHtml(targetLabel)}"
        >
          <span class="month-folder-copy">
            <span class="month-folder-title-row">
              <span class="month-folder-title">${escapeHtml(year)}</span>
              ${isActive ? '<span class="month-folder-badge">当前</span>' : ""}
            </span>
            <span class="month-folder-meta">${escapeHtml(folderMeta)}</span>
          </span>
          <span class="month-folder-trailing">
            <span class="month-folder-count">${escapeHtml(String(items.length))}</span>
            <i class="layui-icon layui-icon-right month-folder-chevron" aria-hidden="true"></i>
          </span>
        </button>
        <div id="${panelId}" class="month-folder-months" ${isActive ? "" : "hidden"}>
          ${monthButtons.join("")}
        </div>
      </div>
    `;
  });

  els.monthYearGroup.innerHTML = yearFolders.join("");
  if (els.monthSegmentGroup) {
    els.monthSegmentGroup.innerHTML = "";
    els.monthSegmentGroup.hidden = true;
  }

  updateMonthNavButton(
    els.monthPrevBtn,
    navigatorModel.olderSnapshot,
    "没有更早月份",
    navigatorModel.olderSnapshot ? `更早 · ${monthLabel(navigatorModel.olderSnapshot)}` : "没有更早月份"
  );
  updateMonthNavButton(
    els.monthNextBtn,
    navigatorModel.newerSnapshot,
    "没有更新月份",
    navigatorModel.newerSnapshot ? `更新 · ${monthLabel(navigatorModel.newerSnapshot)}` : "没有更新月份"
  );
  syncMonthNavigatorViewport(shouldPulseSelection);
  state.monthNavFeedbackSnapshotId = null;
}

function parseReferenceStockValue(snapshot) {
  const raw = `${String(snapshot?.ocrText || "")}\n${String(snapshot?.rawText || "")}`;
  if (!raw.trim()) {
    return null;
  }

  const normalized = raw.replace(/[，,]/g, "").replace(/\s+/g, "");
  const matched = normalized.match(/参考股票市值[^0-9\-]{0,12}([0-9]{4,}(?:\.[0-9]+)?)/);
  if (!matched) {
    return null;
  }

  const value = Number(matched[1]);
  if (!Number.isFinite(value) || value <= 0) {
    return null;
  }

  if (value < 1_000_000 || value > 300_000_000) {
    return null;
  }

  return value;
}

function quantile(sortedValues, q) {
  if (!sortedValues.length) {
    return null;
  }
  const pos = (sortedValues.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  const left = sortedValues[base];
  const right = sortedValues[Math.min(base + 1, sortedValues.length - 1)];
  return left + (right - left) * rest;
}

function sanitizeMarketValues(values) {
  const list = values.filter((value) => Number.isFinite(value) && value > 0).sort((a, b) => a - b);
  if (list.length < 4) {
    return list;
  }

  const q1 = quantile(list, 0.25);
  const q3 = quantile(list, 0.75);
  if (!Number.isFinite(q1) || !Number.isFinite(q3)) {
    return list;
  }

  const iqr = q3 - q1;
  const lower = Math.max(0, q1 - iqr * 3);
  const upper = q3 + iqr * 3;

  const filtered = list.filter((value) => value >= lower && value <= upper);
  return filtered.length >= Math.max(3, Math.floor(list.length * 0.6)) ? filtered : list;
}

function snapshotMarketValueFromRows(snapshot) {
  if (!snapshot) {
    return 0;
  }

  const values = sanitizeMarketValues(
    buildRowsFromSnapshot(snapshot)
      .map((item) => toNumber(item.marketValue))
      .filter((value) => Number.isFinite(value) && value > 0)
  );

  return values.reduce((sum, value) => sum + value, 0);
}

function buildMonthlySeries() {
  const map = new Map();

  for (const snapshot of state.snapshots) {
    const month = monthLabel(snapshot);
    const postMetrics = parsePostMetrics(snapshot);
    const value = resolveSnapshotChartValue(snapshot, null, postMetrics);
    if (!month || month === "-" || !Number.isFinite(value) || value <= 0) {
      continue;
    }

    const existing = map.get(month);
    const time = new Date(snapshot.postedAt || 0).getTime();
    if (!existing || time > existing.time) {
      map.set(month, {
        month,
        value,
        time,
        netIndex: postMetrics.netIndex,
        yearStartNetIndex: postMetrics.yearStartNetIndex
      });
    }
  }

  const series = [...map.values()]
    .sort((a, b) => a.time - b.time)
    .map((item) => ({
      ...item,
      anomaly: false,
      changeRatio: null
    }));

  for (let i = 1; i < series.length; i += 1) {
    const prev = series[i - 1];
    const current = series[i];
    if (!prev || prev.value <= 0) {
      continue;
    }

    const ratio = current.value / prev.value;
    current.changeRatio = ratio;
    if (ratio > 1.8 || ratio < 0.55) {
      current.anomaly = true;
    }
  }

  return series;
}

function setChartEmptyState(message = "") {
  const hasMessage = Boolean(message);
  if (els.monthlyChartEmpty) {
    els.monthlyChartEmpty.textContent = message;
    els.monthlyChartEmpty.classList.toggle("hidden", !hasMessage);
  }
  if (els.monthlyNetworthChart) {
    els.monthlyNetworthChart.classList.toggle("hidden", hasMessage);
  }
  if (hasMessage) {
    if (els.chartSeriesCount) {
      els.chartSeriesCount.textContent = "-";
    }
    if (els.chartLatestMonth) {
      els.chartLatestMonth.textContent = "-";
    }
    if (els.chartLatestValue) {
      els.chartLatestValue.textContent = "-";
    }
    if (els.chartSeriesHint) {
      els.chartSeriesHint.textContent = message;
    }
  }
}

function renderMonthlyChart() {
  if (!els.monthlyNetworthChart) {
    return;
  }

  if (typeof Chart === "undefined") {
    setChartEmptyState("图表加载失败，请刷新后重试");
    return;
  }

  const series = buildMonthlySeries();

  if (state.monthlyChart) {
    state.monthlyChart.destroy();
    state.monthlyChart = null;
  }

  if (series.length === 0) {
    setChartEmptyState("暂无可绘制的月度净值数据");
    const ctx = els.monthlyNetworthChart.getContext("2d");
    if (ctx) {
      ctx.clearRect(0, 0, els.monthlyNetworthChart.width, els.monthlyNetworthChart.height);
    }
    return;
  }

  setChartEmptyState("");

  const latestPoint = series[series.length - 1] || null;
  const anomalyCount = series.filter((item) => item.anomaly).length;
  if (els.chartSeriesCount) {
    els.chartSeriesCount.textContent = `${series.length} 个月`;
  }
  if (els.chartLatestMonth) {
    els.chartLatestMonth.textContent = latestPoint?.month || "-";
  }
  if (els.chartLatestValue) {
    els.chartLatestValue.textContent = latestPoint ? formatCurrency(latestPoint.value, 0) : "-";
  }
  if (els.chartSeriesHint) {
    els.chartSeriesHint.textContent =
      anomalyCount > 0 ? `已标记 ${anomalyCount} 个异常波动点，悬停可查看详情。` : "当前曲线未发现异常波动点。";
  }

  state.monthlyChart = new Chart(els.monthlyNetworthChart, {
    type: "line",
    data: {
      labels: series.map((item) => item.month),
      datasets: [
        {
          label: "总市值",
          data: series.map((item) => item.value),
          borderColor: "#2f6fff",
          backgroundColor: "rgba(47, 111, 255, 0.12)",
          pointBackgroundColor(context) {
            return series[context.dataIndex]?.anomaly ? "#ef6b73" : "#2f6fff";
          },
          pointBorderColor(context) {
            return series[context.dataIndex]?.anomaly ? "#ef6b73" : "#2f6fff";
          },
          pointRadius(context) {
            return series[context.dataIndex]?.anomaly ? 5 : 3;
          },
          pointHoverRadius(context) {
            return series[context.dataIndex]?.anomaly ? 6 : 4;
          },
          borderWidth: 2,
          tension: 0.22,
          fill: true
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        },
        tooltip: {
          callbacks: {
            label(context) {
              const point = series[context.dataIndex];
              const value = Number(context.parsed.y || 0);
              const anomaly = point?.anomaly ? "（异常波动）" : "";
              return `总市值: ¥${value.toLocaleString("zh-CN")}${anomaly}`;
            },
            afterLabel(context) {
              const point = series[context.dataIndex];
              if (!point) {
                return "";
              }

              const lines = [];
              if (Number.isFinite(point.changeRatio)) {
                const percent = ((point.changeRatio - 1) * 100).toFixed(1);
                const sign = Number(percent) > 0 ? "+" : "";
                lines.push(`环比: ${sign}${percent}%`);
              }
              if (Number.isFinite(point.netIndex)) {
                lines.push(`净值指数: ${formatIndex(point.netIndex)}`);
              }
              if (Number.isFinite(point.yearStartNetIndex)) {
                lines.push(`年初净值指数: ${formatIndex(point.yearStartNetIndex)}`);
              }
              return lines;
            }
          }
        }
      },
      scales: {
        x: {
          grid: {
            color: "rgba(144, 154, 170, 0.12)"
          },
          ticks: {
            color: "#6b7383"
          }
        },
        y: {
          grid: {
            color: "rgba(144, 154, 170, 0.12)"
          },
          ticks: {
            color: "#6b7383",
            callback(value) {
              return `¥${Number(value).toLocaleString("zh-CN")}`;
            }
          }
        }
      }
    }
  });
}

function renderAdjustTable(rows) {
  if (!els.adjustRowsBody) {
    return;
  }

  if (rows.length === 0) {
    els.adjustRowsBody.innerHTML = `<tr><td colspan="6" class="replica-empty">暂无调仓数据</td></tr>`;
    return;
  }

  els.adjustRowsBody.innerHTML = renderGroupedRows(rows, 6, (item) => {
      const symbol = escapeHtml(item.symbol);
      const name = escapeHtml(item.name);
      const actionClass = safeBadgeClass(item.actionClass);
      const actionLabel = escapeHtml(item.actionLabel);
      const delta = escapeHtml(formatDelta(item.delta));
      const cost = escapeHtml(formatCost(item.cost));
      const holdingAmount = escapeHtml(item.marketValue === null ? "-" : formatCurrency(item.marketValue, 0));
      const reviewLevel = getRowReviewLevel(item);
      const rowClass = reviewLevel === "none" ? "" : `row-review row-review-${reviewLevel}`;
      return `
        <tr class="${rowClass}">
          <td class="mono">${symbol}</td>
          <td><span class="stock-name">${name}</span></td>
          <td><span class="badge ${actionClass}">${actionLabel}</span></td>
          <td class="mono numeric-cell ${deltaClass(item.delta)}">${delta}</td>
          <td class="mono numeric-cell cost-cell">${cost}</td>
          <td class="mono numeric-cell">${holdingAmount}</td>
        </tr>
      `;
    });
}

function renderOpenTable(rows) {
  if (!els.openRowsBody) {
    return;
  }
  if (rows.length === 0) {
    els.openRowsBody.innerHTML = `<tr><td colspan="4" class="replica-empty">本月暂无新开仓</td></tr>`;
    return;
  }

  els.openRowsBody.innerHTML = renderGroupedRows(rows, 4, (item) => {
      const symbol = escapeHtml(item.symbol);
      const name = escapeHtml(item.name);
      const qty = escapeHtml(`+${formatNumber(item.qty, 0)}`);
      return `
        <tr>
          <td class="mono">${symbol}</td>
          <td><span class="stock-name">${name}</span></td>
          <td class="mono numeric-cell delta-pos">${qty}</td>
          <td class="history-icon">◷</td>
        </tr>
      `;
    });
}

function renderCloseTable(rows) {
  if (!els.closeRowsBody) {
    return;
  }
  if (rows.length === 0) {
    els.closeRowsBody.innerHTML = `<tr><td colspan="4" class="replica-empty">本月暂无清仓</td></tr>`;
    return;
  }

  els.closeRowsBody.innerHTML = renderGroupedRows(rows, 4, (item) => {
      const symbol = escapeHtml(item.symbol);
      const name = escapeHtml(item.name);
      const qty = escapeHtml(formatNumber(item.qty, 0));
      return `
        <tr>
          <td class="mono">${symbol}</td>
          <td><span class="stock-name">${name}</span></td>
          <td class="mono numeric-cell delta-neg">${qty}</td>
          <td class="history-icon">◷</td>
        </tr>
      `;
    });
}

function renderLatestTable(rows) {
  if (!els.latestRowsBody) {
    return;
  }
  if (rows.length === 0) {
    els.latestRowsBody.innerHTML = `<tr><td colspan="5" class="replica-empty">暂无持仓数据</td></tr>`;
    return;
  }

  els.latestRowsBody.innerHTML = renderGroupedRows(rows, 5, (item) => {
      const symbol = escapeHtml(item.symbol);
      const name = escapeHtml(item.name);
      const qty = escapeHtml(formatNumber(item.qty, 0));
      const cost = escapeHtml(formatCost(item.cost));
      const marketValue = escapeHtml(item.marketValue === null ? "-" : formatCost(item.marketValue));
      const reviewLevel = getRowReviewLevel(item);
      const rowClass = reviewLevel === "none" ? "" : `row-review row-review-${reviewLevel}`;
      return `
        <tr class="${rowClass}">
          <td class="mono">${symbol}</td>
          <td><span class="stock-name">${name}</span></td>
          <td class="mono numeric-cell">${qty}</td>
          <td class="mono numeric-cell cost-cell">${cost}</td>
          <td class="mono numeric-cell">${marketValue}</td>
        </tr>
      `;
    });
}

function renderTabState() {
  const isAdjust = state.currentTab === "adjust";

  if (els.tabAdjustBtn) {
    els.tabAdjustBtn.classList.toggle("active", isAdjust);
    els.tabAdjustBtn.setAttribute("aria-selected", String(isAdjust));
    els.tabAdjustBtn.setAttribute("tabindex", isAdjust ? "0" : "-1");
  }
  if (els.tabLatestBtn) {
    els.tabLatestBtn.classList.toggle("active", !isAdjust);
    els.tabLatestBtn.setAttribute("aria-selected", String(!isAdjust));
    els.tabLatestBtn.setAttribute("tabindex", isAdjust ? "-1" : "0");
  }
  if (els.adjustView) {
    els.adjustView.classList.toggle("hidden", !isAdjust);
    els.adjustView.toggleAttribute("hidden", !isAdjust);
    els.adjustView.setAttribute("aria-hidden", String(!isAdjust));
  }
  if (els.latestView) {
    els.latestView.classList.toggle("hidden", isAdjust);
    els.latestView.toggleAttribute("hidden", isAdjust);
    els.latestView.setAttribute("aria-hidden", String(isAdjust));
  }
}

function renderOverviewStats(payload) {
  const current = payload.current;
  const performance = buildPerformanceSummary(payload);
  const updateText = current
    ? `${formatDateTime(current.postedAt)} · ${sourceLabel(current.source)}`
    : "暂无最近更新";

  if (els.totalMarketValue) {
    els.totalMarketValue.textContent = formatCurrency(payload.currentMarketValue, 0);
  }

  if (els.latestMonthValue) {
    els.latestMonthValue.textContent = current ? monthLabel(current) : "-";
  }

  if (els.latestUpdateMeta) {
    els.latestUpdateMeta.textContent = updateText;
  }

  if (els.monthlyReturnValue) {
    els.monthlyReturnValue.textContent =
      performance.profitAmount === null ? "-" : formatCurrency(performance.profitAmount, 0);
    els.monthlyReturnValue.classList.remove("delta-pos", "delta-neg", "delta-zero");
    els.monthlyReturnValue.classList.add(
      performance.profitAmount > 0 ? "delta-pos" : performance.profitAmount < 0 ? "delta-neg" : "delta-zero"
    );
  }

  if (els.monthlyReturnMeta) {
    const parts = [];
    if (performance.profitPct !== null) {
      parts.push(`收益率 ${formatPercent(performance.profitPct)}`);
    }
    if (performance.benchmarkPct !== null) {
      parts.push(`上证 ${formatPercent(performance.benchmarkPct)}`);
    }
    els.monthlyReturnMeta.textContent = parts.length > 0 ? parts.join(" · ") : "暂无收益文本锚点";
  }

  if (els.cumulativeNetValue) {
    els.cumulativeNetValue.textContent = formatCurrency(payload.currentPostMetrics?.cumulativeNetValue, 0);
  }

  if (els.netIndexValue) {
    els.netIndexValue.textContent = formatIndex(payload.currentPostMetrics?.netIndex);
  }

  if (els.yearStartIndexValue) {
    const yearStart = formatIndex(payload.currentPostMetrics?.yearStartNetIndex);
    els.yearStartIndexValue.textContent = yearStart === "-" ? "年初 -" : `年初 ${yearStart}`;
  }

  if (els.holdingCountMeta) {
    els.holdingCountMeta.textContent = `${formatNumber(payload.holdingCount, 0)} 只持仓`;
  }

  if (els.outperformValue) {
    els.outperformValue.textContent = formatPercent(performance.outperformPct);
    els.outperformValue.classList.remove("delta-pos", "delta-neg", "delta-zero");
    els.outperformValue.classList.add(
      performance.outperformPct > 0 ? "delta-pos" : performance.outperformPct < 0 ? "delta-neg" : "delta-zero"
    );
  }

  if (els.outperformMeta) {
    els.outperformMeta.textContent =
      performance.benchmarkPct !== null
        ? `相对上证 ${formatPercent(performance.benchmarkPct)}`
        : "暂无基准指数文本锚点";
  }

  if (els.selectedSnapshotMeta) {
    els.selectedSnapshotMeta.textContent = current
      ? `${monthLabel(current)} · ${sourceLabel(current.source)} · ${formatNumber(payload.holdingCount, 0)} 只持仓 · ${formatDateTime(
          current.postedAt
        )}`
      : "暂无月份数据";
  }
}

function renderAll() {
  const payload = buildRenderPayload();
  renderMonthSelect();
  renderOverviewStats(payload);
  renderAdjustTable(payload.adjustRows);
  renderOpenTable(payload.openRows);
  renderCloseTable(payload.closeRows);
  renderLatestTable(payload.currentRows);
  renderSnapshotDetailList();
  renderTabState();
  renderMonthlyChart();
}

async function loadData() {
  const data = await request("/api/master-snapshots?limit=240");
  const snapshots = Array.isArray(data.snapshots) ? data.snapshots : [];

  state.snapshots = snapshots
    .slice()
    .sort((a, b) => new Date(b.postedAt || 0).getTime() - new Date(a.postedAt || 0).getTime());

  if (state.snapshots.length > 0 && !state.selectedSnapshotId) {
    state.selectedSnapshotId = state.snapshots[0].id;
  }

  if (state.selectedSnapshotId && !state.snapshots.some((item) => item.id === state.selectedSnapshotId)) {
    state.selectedSnapshotId = state.snapshots[0]?.id || null;
  }

  if (state.detailExpandedId && !state.snapshots.some((item) => item.id === state.detailExpandedId)) {
    state.detailExpandedId = null;
  }

  renderAll();
}

function selectSnapshotById(snapshotId, options = {}) {
  const { animateNav = true } = options;
  const value = String(snapshotId || "").trim();
  if (!value || !state.snapshots.some((item) => item.id === value)) {
    return;
  }

  if (state.selectedSnapshotId === value) {
    return;
  }

  state.monthNavFeedbackSnapshotId = animateNav ? value : null;
  state.selectedSnapshotId = value;
  renderAll();
}

function bindEvents() {
  const tabButtons = [els.tabLatestBtn, els.tabAdjustBtn].filter(Boolean);
  const onTabKeydown = (event) => {
    const currentIndex = tabButtons.indexOf(event.currentTarget);
    if (currentIndex < 0) {
      return;
    }

    let nextIndex = currentIndex;
    if (event.key === "ArrowRight") {
      nextIndex = (currentIndex + 1) % tabButtons.length;
    } else if (event.key === "ArrowLeft") {
      nextIndex = (currentIndex - 1 + tabButtons.length) % tabButtons.length;
    } else if (event.key === "Home") {
      nextIndex = 0;
    } else if (event.key === "End") {
      nextIndex = tabButtons.length - 1;
    } else {
      return;
    }

    event.preventDefault();
    const nextTab = tabButtons[nextIndex];
    nextTab.focus();
    nextTab.click();
  };

  if (els.tabLatestBtn) {
    els.tabLatestBtn.addEventListener("click", () => {
      state.currentTab = "latest";
      renderTabState();
    });
    els.tabLatestBtn.addEventListener("keydown", onTabKeydown);
  }

  if (els.tabAdjustBtn) {
    els.tabAdjustBtn.addEventListener("click", () => {
      state.currentTab = "adjust";
      renderTabState();
    });
    els.tabAdjustBtn.addEventListener("keydown", onTabKeydown);
  }

  if (els.snapshotSelect) {
    els.snapshotSelect.addEventListener("change", (event) => {
      const value = String(event.target?.value || "").trim();
      selectSnapshotById(value);
    });
  }

  if (els.monthPrevBtn) {
    els.monthPrevBtn.addEventListener("click", () => {
      const nextSnapshot = olderSnapshot();
      if (nextSnapshot?.id) {
        selectSnapshotById(nextSnapshot.id);
      }
    });
  }

  if (els.monthNextBtn) {
    els.monthNextBtn.addEventListener("click", () => {
      const nextSnapshot = newerSnapshot();
      if (nextSnapshot?.id) {
        selectSnapshotById(nextSnapshot.id);
      }
    });
  }

  if (els.monthYearGroup) {
    els.monthYearGroup.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const monthButton = target.closest("[data-select-snapshot]");
      if (monthButton) {
        const snapshotId = String(monthButton.getAttribute("data-select-snapshot") || "").trim();
        if (snapshotId) {
          selectSnapshotById(snapshotId);
        }
        return;
      }

      const button = target.closest("[data-select-year]");
      if (!button) {
        return;
      }

      const year = String(button.getAttribute("data-select-year") || "").trim();
      if (!year) {
        return;
      }

      const snapshotId = String(button.getAttribute("data-select-year-snapshot") || "").trim();
      const nextSnapshot =
        state.snapshots.find((item) => item.id === snapshotId) || resolveYearSwitchSnapshot(year);
      if (nextSnapshot?.id) {
        selectSnapshotById(nextSnapshot.id);
      }
    });
  }

  if (els.monthSegmentGroup) {
    els.monthSegmentGroup.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const button = target.closest("[data-select-snapshot]");
      if (!button) {
        return;
      }

      const snapshotId = String(button.getAttribute("data-select-snapshot") || "").trim();
      if (snapshotId) {
        selectSnapshotById(snapshotId);
      }
    });
  }

  if (els.detailSnapshotList) {
    els.detailSnapshotList.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }

      const button = target.closest("[data-detail-snapshot]");
      if (!button) {
        return;
      }

      const snapshotId = String(button.getAttribute("data-detail-snapshot") || "").trim();
      if (!snapshotId) {
        return;
      }

      if (state.selectedSnapshotId !== snapshotId) {
        state.monthNavFeedbackSnapshotId = snapshotId;
      }
      state.selectedSnapshotId = snapshotId;
      state.detailExpandedId = state.detailExpandedId === snapshotId ? null : snapshotId;
      renderAll();
    });
  }

  document.addEventListener("keydown", (event) => {
    if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey) {
      return;
    }

    if (isTextInputLike(event.target) || isTextInputLike(document.activeElement)) {
      return;
    }

    if (event.key === "ArrowLeft") {
      const snapshot = olderSnapshot();
      if (!snapshot?.id) {
        return;
      }

      event.preventDefault();
      selectSnapshotById(snapshot.id);
    }

    if (event.key === "ArrowRight") {
      const snapshot = newerSnapshot();
      if (!snapshot?.id) {
        return;
      }

      event.preventDefault();
      selectSnapshotById(snapshot.id);
    }
  });
}

async function bootstrap() {
  bindEvents();
  try {
    await loadData();
  } catch (error) {
    const message = error?.message || "加载失败";
    const adjustRow = `<tr><td colspan="6" class="replica-empty">${escapeHtml(message)}</td></tr>`;
    const latestRow = `<tr><td colspan="5" class="replica-empty">${escapeHtml(message)}</td></tr>`;
    if (els.adjustRowsBody) {
      els.adjustRowsBody.innerHTML = adjustRow;
    }
    if (els.latestRowsBody) {
      els.latestRowsBody.innerHTML = latestRow;
    }
  }
}

bootstrap();
