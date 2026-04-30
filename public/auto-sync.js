const state = {
  autoTracking: null,
  latestSnapshot: null,
  snapshots: [],
  snapshotCache: new Map(),
  snapshotDetailLoadingId: null,
  monthlyUpdates: [],
  monthlyMetricDrafts: new Map(),
  monthlyMetricEditingIds: new Set(),
  monthlyMetricFilter: "all",
  catalogPosts: [],
  selectedSnapshotId: null,
  anomalyReport: null,
  anomalyRowsByKey: new Map(),
  anomalyLoading: false,
  actionBusy: false,
  loadVersion: 0,
  jobOverview: null,
  jobPollTimer: null,
  adminSection: "overview"
};

const els = {
  autoConfigForm: document.getElementById("autoConfigForm"),
  runCookieKeepAliveBtn: document.getElementById("runCookieKeepAliveBtn"),
  runAutoSyncBtn: document.getElementById("runAutoSyncBtn"),
  runBackfillBtn: document.getElementById("runBackfillBtn"),
  loadCatalogBtn: document.getElementById("loadCatalogBtn"),
  importSelectedBtn: document.getElementById("importSelectedBtn"),
  recalculateSnapshotsBtn: document.getElementById("recalculateSnapshotsBtn"),
  catalogCheckAll: document.getElementById("catalogCheckAll"),
  viewLatestBtn: document.getElementById("viewLatestBtn"),
  autoSyncText: document.getElementById("autoSyncText"),
  systemStatusNote: document.getElementById("systemStatusNote"),
  systemStatusGrid: document.getElementById("systemStatusGrid"),
  systemRuntimeStatus: document.getElementById("systemRuntimeStatus"),
  systemLastSuccess: document.getElementById("systemLastSuccess"),
  systemLatestMonth: document.getElementById("systemLatestMonth"),
  systemSnapshotCount: document.getElementById("systemSnapshotCount"),
  systemImportedTrades: document.getElementById("systemImportedTrades"),
  systemOcrStatus: document.getElementById("systemOcrStatus"),
  systemCredentialStatus: document.getElementById("systemCredentialStatus"),
  systemLastError: document.getElementById("systemLastError"),
  contextCurrentMonth: document.getElementById("contextCurrentMonth"),
  contextCatalogSelected: document.getElementById("contextCatalogSelected"),
  contextMetricsDirty: document.getElementById("contextMetricsDirty"),
  contextAnomalyCount: document.getElementById("contextAnomalyCount"),
  latestSnapshotMeta: document.getElementById("latestSnapshotMeta"),
  masterRowsBody: document.getElementById("masterRowsBody"),
  syncLogsBody: document.getElementById("syncLogsBody"),
  catalogMeta: document.getElementById("catalogMeta"),
  catalogBody: document.getElementById("catalogBody"),
  snapshotHistoryMeta: document.getElementById("snapshotHistoryMeta"),
  snapshotHistoryBody: document.getElementById("snapshotHistoryBody"),
  anomalyMeta: document.getElementById("anomalyMeta"),
  anomalyRowsBody: document.getElementById("anomalyRowsBody"),
  monthlyMetricMeta: document.getElementById("monthlyMetricMeta"),
  monthlyMetricBody: document.getElementById("monthlyMetricBody"),
  monthlyMetricToolbarText: document.getElementById("monthlyMetricToolbarText"),
  monthlyMetricFilters: document.getElementById("monthlyMetricFilters"),
  saveAllMetricsBtn: document.getElementById("saveAllMetricsBtn"),
  discardAllMetricsBtn: document.getElementById("discardAllMetricsBtn"),
  jobStatusCard: document.getElementById("jobStatusCard"),
  jobStatusTitle: document.getElementById("jobStatusTitle"),
  jobStatusSummary: document.getElementById("jobStatusSummary"),
  jobStatusProgress: document.getElementById("jobStatusProgress"),
  jobStatusMeta: document.getElementById("jobStatusMeta")
};

const ADMIN_SECTIONS = new Set(["overview", "config", "tasks", "review", "metrics"]);

function setAdminSection(section) {
  const nextSection = ADMIN_SECTIONS.has(section) ? section : "overview";
  state.adminSection = nextSection;

  for (const button of document.querySelectorAll("[data-admin-section]")) {
    const active = button.getAttribute("data-admin-section") === nextSection;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  }

  for (const panel of document.querySelectorAll("[data-admin-panel]")) {
    const active = panel.getAttribute("data-admin-panel") === nextSection;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  }

  const url = new URL(window.location.href);
  url.searchParams.set("section", nextSection);
  window.history.replaceState({}, "", url.toString());
}

function initAdminSectionFromUrl() {
  const url = new URL(window.location.href);
  setAdminSection(url.searchParams.get("section") || "overview");
}

const MONTHLY_METRIC_FIELDS = Object.freeze([
  {
    key: "netValueWan",
    digits: 2,
    step: "0.01",
    manualMetricKey: "cumulativeNetValue",
    updateFieldKey: "netValue"
  },
  {
    key: "netIndex",
    digits: 4,
    step: "0.0001",
    manualMetricKey: "netIndex",
    updateFieldKey: "netIndex"
  },
  {
    key: "yearStartNetIndex",
    digits: 4,
    step: "0.0001",
    manualMetricKey: "yearStartNetIndex",
    updateFieldKey: "yearStartNetIndex"
  }
]);

function formatDateTime(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatCardDateTime(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return date.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function formatNumber(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return "-";
  }

  return num.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatInputNumber(value, digits = 2) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return "";
  }

  const factor = 10 ** digits;
  return String(Math.round(num * factor) / factor);
}

function formatMetricHint(value, digits = 2) {
  const formatted = formatInputNumber(value, digits);
  return formatted || "-";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeExternalUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  try {
    const url = new URL(text, window.location.origin);
    if (!["http:", "https:"].includes(url.protocol)) {
      return "";
    }
    return url.toString();
  } catch {
    return "";
  }
}

function buildExternalLinkHtml(value, label = "打开") {
  const normalized = normalizeExternalUrl(value);
  if (!normalized) {
    return "-";
  }

  return `<a href="${escapeHtml(normalized)}" target="_blank" rel="noreferrer">${escapeHtml(label)}</a>`;
}

function monthLabelByDate(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function monthLabelFromTitleOrDate(title, postedAt) {
  const text = String(title || "");
  const matched = text.match(/(20\d{2})\s*年\s*(\d{1,2})\s*月/);
  if (matched) {
    const year = matched[1];
    const month = String(Number(matched[2])).padStart(2, "0");
    return `${year}-${month}`;
  }
  return monthLabelByDate(postedAt);
}

function formatSourceLabel(value) {
  if (value === "xueqiu") {
    return "雪球";
  }
  if (value === "weibo") {
    return "微博";
  }
  if (value === "both") {
    return "双源";
  }
  return value || "-";
}

function formatOcrProviderLabel(value) {
  if (value === "qwen") {
    return "Qwen OCR";
  }
  if (value === "local") {
    return "本地 Tesseract";
  }
  return "自动模式";
}

function formatAutoSchedulePolicy(config) {
  if (!config) {
    return "-";
  }
  if (!config.smartScheduleEnabled) {
    return `固定 ${Number(config.intervalMinutes) || 180} 分钟`;
  }

  return `月底 ${Number(config.monthEndWindowDays) || 2} 天内 ${Number(config.intervalMinutes) || 180} 分钟 / 平时 ${
    Number(config.offWindowIntervalHours) || 72
  } 小时`;
}

function formatCookieKeepAlivePolicy(config) {
  if (!config?.cookieKeepAliveEnabled) {
    return "Cookie 保活关闭";
  }

  return `Cookie 保活 ${Number(config.cookieKeepAliveIntervalHours) || 12} 小时`;
}

function formatSavedCookieUsage(sourceLabel, hasSavedCookie, fallbackLabel = "相关抓取") {
  if (hasSavedCookie) {
    return `${sourceLabel} Cookie 使用已保存值`;
  }

  return `${sourceLabel} Cookie 未保存，${fallbackLabel}会跳过`;
}

function collectCookieSkipTips(logs) {
  const items = Array.isArray(logs) ? logs : [];
  const tips = new Set();

  for (const item of items) {
    const message = String(item?.message || "").trim();
    if (!message || !message.includes("Cookie")) {
      continue;
    }

    if (message.includes("雪球")) {
      if (message.includes("历史回溯")) {
        tips.add("未保存雪球 Cookie，历史回溯已跳过");
        continue;
      }
      if (message.includes("置顶链接")) {
        tips.add("未保存雪球 Cookie，雪球置顶链接已跳过");
        continue;
      }
      if (message.includes("时间线")) {
        tips.add("未保存雪球 Cookie，雪球时间线已跳过");
        continue;
      }
      if (message.includes("示例值")) {
        tips.add("雪球 Cookie 当前仍是示例值，相关雪球抓取已跳过");
        continue;
      }
      tips.add("雪球 Cookie 不可用，相关雪球抓取已跳过");
      continue;
    }

    if (message.includes("微博")) {
      if (message.includes("置顶链接")) {
        tips.add("未保存微博 Cookie，微博置顶链接已跳过");
        continue;
      }
      if (message.includes("时间线")) {
        tips.add("未保存微博 Cookie，微博时间线已跳过");
        continue;
      }
      if (message.includes("示例值")) {
        tips.add("微博 Cookie 当前仍是示例值，相关微博抓取已跳过");
        continue;
      }
      tips.add("微博 Cookie 不可用，相关微博抓取已跳过");
    }
  }

  return [...tips];
}

function formatCookieSkipSuffix(logs) {
  const tips = collectCookieSkipTips(logs);
  if (tips.length === 0) {
    return "";
  }

  return `；注意：${tips.slice(0, 2).join("；")}`;
}

function formatNameSourceLabel(value) {
  const source = String(value || "").trim().toLowerCase();
  if (source === "xueqiu") {
    return "雪球";
  }
  if (source === "tencent") {
    return "腾讯备份";
  }
  if (source) {
    return source;
  }
  return "本地纠错";
}

function formatNameSourceTagClass(value) {
  const source = String(value || "").trim().toLowerCase();
  if (source === "xueqiu") {
    return "tag-source-xueqiu";
  }
  if (source === "tencent") {
    return "tag-source-tencent";
  }
  return "tag-source-local";
}

function normalizePostId(value) {
  return String(value || "").trim();
}

function buildAnomalyKey(postId, symbol) {
  return `${normalizePostId(postId)}|${String(symbol || "").trim().toUpperCase()}`;
}

function getSnapshotAnomalyCount(postId) {
  const snapshots = Array.isArray(state.anomalyReport?.snapshots) ? state.anomalyReport.snapshots : [];
  const snapshot = snapshots.find((item) => normalizePostId(item.postId) === normalizePostId(postId));
  return Array.isArray(snapshot?.rows) ? snapshot.rows.length : 0;
}

function getRowDiagnostics(snapshot, row) {
  if (!snapshot || !row) {
    return null;
  }
  return state.anomalyRowsByKey.get(buildAnomalyKey(snapshot.postId, row.symbol)) || null;
}

function formatSnapshotValueSummary(row) {
  const qty = Number.isFinite(Number(row?.holdingQty))
    ? Number(row.holdingQty)
    : Number.isFinite(Number(row?.changeQty))
      ? Number(row.changeQty)
      : null;
  const cost = Number.isFinite(Number(row?.referenceCost))
    ? Number(row.referenceCost)
    : Number.isFinite(Number(row?.latestCost))
      ? Number(row.latestCost)
      : null;
  const latestPrice = Number.isFinite(Number(row?.latestPrice)) ? Number(row.latestPrice) : null;
  const marketValue = Number.isFinite(Number(row?.marketValue)) ? Number(row.marketValue) : null;

  return [
    qty === null ? null : `持仓 ${formatNumber(qty, 0)}`,
    cost === null ? null : `成本 ${formatNumber(cost, 3)}`,
    latestPrice === null ? null : `最新 ${formatNumber(latestPrice, 3)}`,
    marketValue === null ? null : `市值 ${formatNumber(marketValue, 2)}`
  ]
    .filter(Boolean)
    .join(" / ");
}

function getCatalogCheckboxes() {
  if (!els.catalogBody) {
    return [];
  }

  return Array.from(els.catalogBody.querySelectorAll("input[data-catalog-id]"));
}

function syncCatalogCheckAllState() {
  if (!els.catalogCheckAll) {
    return;
  }

  const selectable = getCatalogCheckboxes().filter((input) => !input.disabled);
  const selectedCount = selectable.filter((input) => input.checked).length;

  if (selectable.length === 0) {
    els.catalogCheckAll.checked = false;
    els.catalogCheckAll.indeterminate = false;
    els.catalogCheckAll.disabled = true;
    return;
  }

  els.catalogCheckAll.disabled = false;
  els.catalogCheckAll.checked = selectedCount > 0 && selectedCount === selectable.length;
  els.catalogCheckAll.indeterminate = selectedCount > 0 && selectedCount < selectable.length;
}

function updateCatalogMeta() {
  if (!els.catalogMeta) {
    return;
  }

  const selectable = getCatalogCheckboxes().filter((input) => !input.disabled);
  const selectedCount = selectable.filter((input) => input.checked).length;
  els.catalogMeta.textContent = `共 ${state.catalogPosts.length} 条，可选 ${selectable.length} 条，已选 ${selectedCount} 条`;
  renderWorkContext();
}

function getSnapshotHistoryList() {
  return Array.isArray(state.snapshots) ? state.snapshots : [];
}

function getSnapshotRowCount(snapshot) {
  if (!snapshot || typeof snapshot !== "object") {
    return 0;
  }

  const explicit = Number(snapshot.rowCount);
  if (Number.isFinite(explicit) && explicit >= 0) {
    return explicit;
  }

  return Array.isArray(snapshot.rows) ? snapshot.rows.length : 0;
}

function getViewingSnapshot() {
  if (state.selectedSnapshotId) {
    if (state.latestSnapshot && state.latestSnapshot.id === state.selectedSnapshotId) {
      return state.latestSnapshot;
    }
    return state.snapshotCache.get(state.selectedSnapshotId) || null;
  }
  return state.latestSnapshot;
}

function rebuildAnomalyIndex() {
  state.anomalyRowsByKey = new Map();

  for (const snapshot of state.anomalyReport?.snapshots || []) {
    for (const row of snapshot.rows || []) {
      if (!row.diagnostics) {
        continue;
      }
      state.anomalyRowsByKey.set(buildAnomalyKey(snapshot.postId, row.symbol), row.diagnostics);
    }
  }
}

function syncSnapshotCache() {
  const validIds = new Set(
    getSnapshotHistoryList()
      .map((item) => String(item?.id || "").trim())
      .filter(Boolean)
  );
  const nextCache = new Map();

  for (const [id, snapshot] of state.snapshotCache.entries()) {
    if (validIds.has(id)) {
      nextCache.set(id, snapshot);
    }
  }

  if (state.latestSnapshot?.id) {
    nextCache.set(String(state.latestSnapshot.id), state.latestSnapshot);
  }

  state.snapshotCache = nextCache;
}

function hasManualMetrics(value) {
  if (!value || typeof value !== "object") {
    return false;
  }

  return ["cumulativeNetValue", "netIndex", "yearStartNetIndex"].some((key) => {
    const num = Number(value[key]);
    return Number.isFinite(num) && num > 0;
  });
}

function buildPersistedMonthlyMetricDraft(update) {
  const manualMetrics = hasManualMetrics(update?.manualMetrics) ? update.manualMetrics : null;

  return {
    netValueWan:
      manualMetrics && Number.isFinite(Number(manualMetrics.cumulativeNetValue))
        ? formatInputNumber(Number(manualMetrics.cumulativeNetValue) / 10_000, 2)
        : "",
    netIndex:
      manualMetrics && Number.isFinite(Number(manualMetrics.netIndex))
        ? formatInputNumber(manualMetrics.netIndex, 4)
        : "",
    yearStartNetIndex:
      manualMetrics && Number.isFinite(Number(manualMetrics.yearStartNetIndex))
        ? formatInputNumber(manualMetrics.yearStartNetIndex, 4)
        : ""
  };
}

function normalizeDraftFieldValue(value) {
  return String(value ?? "").trim();
}

function getMonthlyMetricDraft(update) {
  if (!update) {
    return null;
  }

  return state.monthlyMetricDrafts.get(update.id) || buildPersistedMonthlyMetricDraft(update);
}

function setMonthlyMetricDraft(updateId, draft) {
  state.monthlyMetricDrafts.set(updateId, {
    netValueWan: normalizeDraftFieldValue(draft?.netValueWan),
    netIndex: normalizeDraftFieldValue(draft?.netIndex),
    yearStartNetIndex: normalizeDraftFieldValue(draft?.yearStartNetIndex)
  });
}

function clearMonthlyMetricDraft(updateId) {
  state.monthlyMetricDrafts.delete(updateId);
}

function isMonthlyMetricDraftDirty(update, draft) {
  const persisted = buildPersistedMonthlyMetricDraft(update);
  const current = draft || getMonthlyMetricDraft(update);
  if (!current) {
    return false;
  }

  return MONTHLY_METRIC_FIELDS.some(
    (field) => normalizeDraftFieldValue(current[field.key]) !== normalizeDraftFieldValue(persisted[field.key])
  );
}

function hasAnyMonthlyMetricDraftValue(draft) {
  if (!draft) {
    return false;
  }

  return MONTHLY_METRIC_FIELDS.some((field) => Boolean(normalizeDraftFieldValue(draft[field.key])));
}

function getMonthlyUpdateById(updateId) {
  return (state.monthlyUpdates || []).find((item) => item.id === updateId) || null;
}

function getDirtyMonthlyMetricIds() {
  return (state.monthlyUpdates || [])
    .filter((update) => isMonthlyMetricDraftDirty(update, getMonthlyMetricDraft(update)))
    .map((update) => update.id);
}

function reconcileMonthlyMetricUiState() {
  const validIds = new Set((state.monthlyUpdates || []).map((item) => item.id));

  for (const draftId of Array.from(state.monthlyMetricDrafts.keys())) {
    if (!validIds.has(draftId)) {
      state.monthlyMetricDrafts.delete(draftId);
    }
  }

  state.monthlyMetricEditingIds = new Set(
    Array.from(state.monthlyMetricEditingIds).filter((updateId) => validIds.has(updateId))
  );
}

function syncMonthlyMetricToolbarState() {
  const updates = Array.isArray(state.monthlyUpdates) ? state.monthlyUpdates : [];
  const dirtyIds = getDirtyMonthlyMetricIds();
  const editingIdSet = state.monthlyMetricEditingIds;
  const manualCount = updates.filter((item) => hasManualMetrics(item.manualMetrics)).length;

  if (els.monthlyMetricToolbarText) {
    if (dirtyIds.length > 0) {
      els.monthlyMetricToolbarText.textContent = `已编辑 ${editingIdSet.size} 条，待保存 ${dirtyIds.length} 条`;
    } else if (editingIdSet.size > 0) {
      els.monthlyMetricToolbarText.textContent = `已进入编辑 ${editingIdSet.size} 条，尚未产生待保存修改`;
    } else {
      els.monthlyMetricToolbarText.textContent = "未开始编辑";
    }
  }

  if (els.saveAllMetricsBtn) {
    els.saveAllMetricsBtn.disabled = dirtyIds.length === 0;
  }

  if (els.discardAllMetricsBtn) {
    els.discardAllMetricsBtn.disabled = dirtyIds.length === 0 && editingIdSet.size === 0;
  }

  if (els.monthlyMetricFilters) {
    const counts = {
      all: updates.length,
      dirty: dirtyIds.length,
      manual: manualCount
    };
    const labels = {
      all: "全部",
      dirty: "待保存",
      manual: "人工校正"
    };

    for (const button of Array.from(els.monthlyMetricFilters.querySelectorAll("[data-metric-filter]"))) {
      const filter = String(button.getAttribute("data-metric-filter") || "").trim();
      button.classList.toggle("is-active", state.monthlyMetricFilter === filter);
      button.textContent = `${labels[filter] || filter} ${counts[filter] ?? 0}`;
    }
  }

  renderWorkContext();
  return dirtyIds;
}

function getSelectedCatalogCount() {
  return getCatalogCheckboxes().filter((input) => !input.disabled && input.checked).length;
}

function renderWorkContext() {
  const selectedSummary =
    state.selectedSnapshotId && Array.isArray(state.snapshots)
      ? state.snapshots.find((item) => String(item?.id || "").trim() === String(state.selectedSnapshotId || "").trim()) || null
      : null;
  const currentSnapshot = getViewingSnapshot() || selectedSummary || state.latestSnapshot || state.snapshots[0] || null;
  const currentMonth = currentSnapshot ? monthLabelFromTitleOrDate(currentSnapshot.title, currentSnapshot.postedAt) : "-";
  const selectedCatalogCount = getSelectedCatalogCount();
  const dirtyCount = getDirtyMonthlyMetricIds().length;
  const anomalySnapshots = Array.isArray(state.anomalyReport?.snapshots)
    ? state.anomalyReport.snapshots.filter((snapshot) => (snapshot.rows || []).some((row) => (row.diagnostics?.issues || []).length > 0))
    : [];
  const anomalySummary = state.anomalyReport?.summary || {};
  const issueRowCount = Number(anomalySummary.issueRowCount) || 0;
  const issueSnapshotCount = anomalySnapshots.length;

  if (els.contextCurrentMonth) {
    els.contextCurrentMonth.textContent = currentMonth;
  }
  if (els.contextCatalogSelected) {
    els.contextCatalogSelected.textContent = `${selectedCatalogCount} 条`;
  }
  if (els.contextMetricsDirty) {
    els.contextMetricsDirty.textContent = `${dirtyCount} 条`;
  }
  if (els.contextAnomalyCount) {
    els.contextAnomalyCount.textContent =
      issueRowCount > 0 ? `${issueRowCount} 行 / ${issueSnapshotCount} 月` : "0 行";
  }
}

function getVisibleMonthlyUpdates(list, dirtyIdSet) {
  const updates = Array.isArray(list) ? list : [];

  if (state.monthlyMetricFilter === "dirty") {
    return updates.filter((item) => dirtyIdSet.has(item.id));
  }

  if (state.monthlyMetricFilter === "manual") {
    return updates.filter((item) => hasManualMetrics(item.manualMetrics));
  }

  return updates;
}

function getMonthlyMetricEmptyMessage() {
  if (state.monthlyMetricFilter === "dirty") {
    return "当前没有待保存的月度指标。";
  }

  if (state.monthlyMetricFilter === "manual") {
    return "当前没有人工校正的月度指标。";
  }

  return "暂无可校正的月度指标。";
}

async function request(url, options = {}) {
  let response;
  try {
    response = await fetch(url, {
      ...options,
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {})
      }
    });
  } catch (error) {
    throw new Error("无法连接本地服务，请确认 `npm start` 正在运行");
  }

  if (response.status === 401) {
    const next = encodeURIComponent(window.location.pathname + window.location.search);
    window.location.replace(`/admin-login.html?next=${next}`);
    throw new Error("后台未登录或登录已过期，请重新登录");
  }

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || `请求失败 (${response.status})`);
  }

  return data;
}

function setStatus(text, level = "info") {
  if (!els.autoSyncText) {
    return;
  }

  els.autoSyncText.textContent = text;
  els.autoSyncText.classList.remove("pos", "neg");
  if (level === "ok") {
    els.autoSyncText.classList.add("pos");
  }
  if (level === "err") {
    els.autoSyncText.classList.add("neg");
  }
}

function setActionBusy(isBusy) {
  state.actionBusy = Boolean(isBusy);

  [
    els.runCookieKeepAliveBtn,
    els.runAutoSyncBtn,
    els.runBackfillBtn,
    els.loadCatalogBtn,
    els.importSelectedBtn,
    els.recalculateSnapshotsBtn
  ]
    .filter(Boolean)
    .forEach((button) => {
      button.disabled = state.actionBusy;
    });
}

function setStatusCardTone(element, tone) {
  const card = element?.closest?.(".system-status-card");
  if (!card) {
    return;
  }

  card.classList.remove("tone-ok", "tone-warn", "tone-err", "tone-neutral");
  card.classList.add(tone || "tone-neutral");
}

function resolveAutoTrackingResultStatus(result, successPrefix) {
  if (result?.error) {
    return {
      level: "err",
      text: `${successPrefix}失败: ${result.error}`
    };
  }

  if (result?.skipped) {
    return {
      level: "err",
      text: result.reason || "任务正在执行中，请稍后再试"
    };
  }

  const importedSnapshots = Number(result?.importedSnapshots) || 0;
  const importedTrades = Number(result?.importedTrades) || 0;
  const cookieSuffix = formatCookieSkipSuffix(result?.logs);
  const noImportedData = importedSnapshots <= 0 && importedTrades <= 0;
  return {
    level: noImportedData && cookieSuffix ? "err" : "ok",
    text: noImportedData
      ? `${successPrefix}完成：未导入新数据${cookieSuffix}`
      : `${successPrefix}完成：快照 ${importedSnapshots} 条，交易 ${importedTrades} 条${cookieSuffix}`
  };
}

function resolveCookieKeepAliveResultStatus(result) {
  if (result?.error) {
    return {
      level: "err",
      text: `Cookie 保活失败: ${result.error}`
    };
  }

  if (result?.skipped) {
    return {
      level: "err",
      text: result.reason || "当前没有可执行的 Cookie 保活目标"
    };
  }

  const successCount = Number(result?.successCount) || 0;
  const failedCount = Number(result?.failedCount) || 0;
  if (failedCount > 0 && successCount > 0) {
    return {
      level: "err",
      text: `Cookie 保活部分成功：成功 ${successCount} 个，失败 ${failedCount} 个`
    };
  }
  if (failedCount > 0) {
    return {
      level: "err",
      text: `Cookie 保活失败：失败 ${failedCount} 个`
    };
  }

  return {
    level: "ok",
    text: `Cookie 保活完成：成功 ${successCount} 个`
  };
}

function renderForm() {
  const config = state.autoTracking?.config;
  if (!config || !els.autoConfigForm) {
    return;
  }

  const setValue = (name, value) => {
    const input = els.autoConfigForm.elements?.[name];
    if (!input) {
      return;
    }
    input.value = value;
  };

  const setPlaceholder = (name, value) => {
    const input = els.autoConfigForm.elements?.[name];
    if (!input) {
      return;
    }
    input.setAttribute("placeholder", value);
  };

  setValue("enabled", String(Boolean(config.enabled)));
  setValue("intervalMinutes", String(config.intervalMinutes || 180));
  setValue("smartScheduleEnabled", String(config.smartScheduleEnabled !== false));
  setValue("monthEndWindowDays", String(config.monthEndWindowDays || 2));
  setValue("offWindowIntervalHours", String(config.offWindowIntervalHours || 72));
  setValue("skipStartupOutsideWindow", String(config.skipStartupOutsideWindow !== false));
  setValue("cookieKeepAliveEnabled", String(config.cookieKeepAliveEnabled !== false));
  setValue("cookieKeepAliveIntervalHours", String(config.cookieKeepAliveIntervalHours || 12));
  setValue("maxPostsPerSource", String(config.maxPostsPerSource || 6));
  setValue("ocrEnabled", String(Boolean(config.ocrEnabled)));
  setValue("ocrProvider", String(config.ocrProvider || "auto"));
  setValue("ocrMaxImagesPerPost", String(config.ocrMaxImagesPerPost || 1));
  setValue("pinnedPostUrls", Array.isArray(config.pinnedPostUrls) ? config.pinnedPostUrls.join("\n") : "");
  setValue("xueqiuTitleRegex", String(config.xueqiuTitleRegex || ""));
  setValue("backfillMaxPages", String(config.backfillMaxPages || 36));
  setValue("backfillPageSize", String(config.backfillPageSize || 20));
  setValue("keywords", Array.isArray(config.keywords) ? config.keywords.join(",") : "");
  setPlaceholder(
    "qwenApiKey",
    config.hasQwenApiKey
      ? "已保存 Qwen Key，留空则保持不变；如需替换，直接输入新 Key"
      : "留空则保持已保存值，或读取环境变量 DASHSCOPE_API_KEY"
  );
  setPlaceholder(
    "xueqiuCookie",
    config.hasXueqiuCookie
      ? "已保存雪球 Cookie，留空则保持不变；如需替换，直接粘贴新 Cookie"
      : "粘贴完整 Cookie；留空则保持已保存值"
  );
  setPlaceholder(
    "weiboCookie",
    config.hasWeiboCookie
      ? "已保存微博 Cookie，留空则保持不变；如需替换，直接粘贴新 Cookie"
      : "粘贴完整 Cookie；留空则保持已保存值"
  );

  const runtime = state.autoTracking?.runtime || {};
  const pinnedCount = Array.isArray(config.pinnedPostUrls) ? config.pinnedPostUrls.length : 0;
  const regexText = config.xueqiuTitleRegex ? ` / 标题规则:${config.xueqiuTitleRegex}` : "";
  const ocrProviderText =
    config.ocrProvider === "qwen"
      ? "Qwen OCR 优先"
      : config.ocrProvider === "local"
        ? "本地 Tesseract"
        : "自动(Qwen优先)";
  const scheduleText = runtime.scheduleHint || `调度策略:${formatAutoSchedulePolicy(config)}`;
  const cookieKeepAliveText = formatCookieKeepAlivePolicy(config);
  const cookieText = `${formatSavedCookieUsage("雪球", config.hasXueqiuCookie, "雪球置顶、时间线与历史回溯")} / ${formatSavedCookieUsage(
    "微博",
    config.hasWeiboCookie,
    "微博相关抓取"
  )} / QwenKey:${config.hasQwenApiKey ? "已配置" : "未配置"} / OCR:${ocrProviderText} / ${cookieKeepAliveText} / 置顶链接:${pinnedCount}条 / ${scheduleText}${regexText}`;
  const runText = runtime.lastRunAt ? `最近执行: ${formatDateTime(runtime.lastRunAt)}` : "尚未执行";
  const keepAliveRunText = runtime.lastCookieKeepAliveSuccessAt
    ? `最近保活: ${formatDateTime(runtime.lastCookieKeepAliveSuccessAt)}`
    : "尚未保活";
  const errText = runtime.lastError ? ` | 最近抓取错误: ${runtime.lastError}` : "";
  const keepAliveErrText = runtime.lastCookieKeepAliveError
    ? ` | 最近保活错误: ${runtime.lastCookieKeepAliveError}`
    : "";
  setStatus(
    `${cookieText} | ${runText} | ${keepAliveRunText}${errText}${keepAliveErrText}`,
    runtime.lastError || runtime.lastCookieKeepAliveError ? "err" : "ok"
  );
}

function renderSystemStatus() {
  if (!els.systemStatusGrid) {
    return;
  }

  const config = state.autoTracking?.config || {};
  const runtime = state.autoTracking?.runtime || {};
  const latestSnapshot = state.latestSnapshot || state.snapshots[0] || null;
  const totalSnapshots = Number(runtime.totalImportedSnapshots) || state.snapshots.length || 0;
  const totalImportedTrades = Number(runtime.totalImportedTrades) || 0;
  const latestMonth = latestSnapshot ? monthLabelFromTitleOrDate(latestSnapshot.title, latestSnapshot.postedAt) : "-";
  const latestRowCount = Array.isArray(latestSnapshot?.rows) ? latestSnapshot.rows.length : getSnapshotRowCount(latestSnapshot);
  const anomalySummary = state.anomalyReport?.summary || {};
  const issueRowCount = Number(anomalySummary.issueRowCount) || 0;
  const manualMetricCount = (state.monthlyUpdates || []).filter((item) => hasManualMetrics(item.manualMetrics)).length;

  let runtimeText = "已暂停";
  let runtimeTone = "tone-warn";
  if (runtime.lastError) {
    runtimeText = "存在异常";
    runtimeTone = "tone-err";
  } else if (config.enabled) {
    runtimeText = "运行正常";
    runtimeTone = "tone-ok";
  }

  const ocrText = config.ocrEnabled ? formatOcrProviderLabel(config.ocrProvider) : "已关闭";
  const credentialText = [
    config.hasXueqiuCookie ? "雪球 已保存" : "雪球 未保存",
    config.hasWeiboCookie ? "微博 已保存" : "微博 未保存",
    `Qwen ${config.hasQwenApiKey ? "已配" : "未配"}`,
    config.cookieKeepAliveEnabled ? `保活 ${Number(config.cookieKeepAliveIntervalHours) || 12}h` : "保活 关闭"
  ].join(" / ");
  const noteParts = [
    runtime.lastRunAt ? `最近执行 ${formatDateTime(runtime.lastRunAt)}` : "尚未执行",
    runtime.nextRunAt ? `下次 ${formatDateTime(runtime.nextRunAt)}` : config.enabled ? "等待下一轮调度" : "自动同步已关闭",
    runtime.nextCookieKeepAliveAt
      ? `保活下次 ${formatDateTime(runtime.nextCookieKeepAliveAt)}`
      : config.cookieKeepAliveEnabled
        ? "等待 Cookie 保活调度"
        : "Cookie 保活已关闭",
    !config.hasXueqiuCookie ? "未保存雪球 Cookie，雪球置顶/时间线/回溯会跳过" : "雪球抓取使用已保存 Cookie",
    !config.hasWeiboCookie ? "未保存微博 Cookie，微博相关抓取会跳过" : "微博抓取使用已保存 Cookie",
    runtime.scheduleHint || `调度 ${formatAutoSchedulePolicy(config)}`,
    `月度指标 ${state.monthlyUpdates.length} 条`,
    issueRowCount > 0 ? `待复核 ${issueRowCount} 行` : "异常检查正常"
  ];

  if (els.systemStatusNote) {
    els.systemStatusNote.textContent = noteParts.join(" · ");
  }
  if (els.systemRuntimeStatus) {
    els.systemRuntimeStatus.textContent = runtimeText;
    setStatusCardTone(els.systemRuntimeStatus, runtimeTone);
  }
  if (els.systemLastSuccess) {
    els.systemLastSuccess.textContent = formatCardDateTime(runtime.lastSuccessAt);
    setStatusCardTone(els.systemLastSuccess, runtime.lastSuccessAt ? "tone-ok" : "tone-warn");
  }
  if (els.systemLatestMonth) {
    els.systemLatestMonth.textContent = latestMonth;
    setStatusCardTone(els.systemLatestMonth, latestSnapshot ? "tone-ok" : "tone-warn");
  }
  if (els.systemSnapshotCount) {
    els.systemSnapshotCount.textContent = `${totalSnapshots} 条`;
    setStatusCardTone(els.systemSnapshotCount, totalSnapshots > 0 ? "tone-ok" : "tone-warn");
  }
  if (els.systemImportedTrades) {
    els.systemImportedTrades.textContent = `${totalImportedTrades} 笔`;
    setStatusCardTone(els.systemImportedTrades, totalImportedTrades > 0 ? "tone-ok" : "tone-neutral");
  }
  if (els.systemOcrStatus) {
    els.systemOcrStatus.textContent = ocrText;
    setStatusCardTone(els.systemOcrStatus, config.ocrEnabled ? "tone-ok" : "tone-warn");
  }
  if (els.systemCredentialStatus) {
    els.systemCredentialStatus.textContent = credentialText;
    setStatusCardTone(
      els.systemCredentialStatus,
      config.hasXueqiuCookie || config.hasWeiboCookie ? "tone-ok" : "tone-warn"
    );
  }
  if (els.systemLastError) {
    els.systemLastError.textContent =
      runtime.lastError || runtime.lastCookieKeepAliveError || "当前无错误记录";
    setStatusCardTone(els.systemLastError, runtime.lastError || runtime.lastCookieKeepAliveError ? "tone-err" : "tone-ok");
  }

  const cardMetaMap = new Map([
    [els.systemRuntimeStatus, config.enabled ? (runtime.scheduleHint || formatAutoSchedulePolicy(config)) : "自动同步已关闭"],
    [els.systemLastSuccess, runtime.lastRunAt ? `最近执行 ${formatDateTime(runtime.lastRunAt)}` : "尚未触发抓取"],
    [
      els.systemLatestMonth,
      latestSnapshot ? `${formatSourceLabel(latestSnapshot.source)} · ${latestRowCount || 0} 行结构化持仓` : "尚未导入最新月份"
    ],
    [els.systemSnapshotCount, `当前历史列表 ${state.snapshots.length} 条`],
    [els.systemImportedTrades, manualMetricCount > 0 ? `人工校正 ${manualMetricCount} 条` : "暂无人工校正记录"],
    [els.systemOcrStatus, config.ocrEnabled ? `每帖最多识别 ${Number(config.ocrMaxImagesPerPost) || 1} 张图片` : "当前不会执行 OCR"],
    [
      els.systemCredentialStatus,
      config.hasXueqiuCookie || config.hasWeiboCookie
        ? "Cookie 输入框留空时，系统会继续使用已保存值"
        : "当前未保存 Cookie，对应站点抓取会自动跳过"
    ],
    [
      els.systemLastError,
      runtime.lastError || runtime.lastCookieKeepAliveError
        ? "建议检查 Cookie、标题规则、OCR 结果或站点登录态"
        : "最近导入链路与 Cookie 保活都比较稳定"
    ]
  ]);

  for (const [element, text] of cardMetaMap.entries()) {
    const meta = element?.closest?.(".system-status-card")?.querySelector(".system-status-meta");
    if (meta && !element?.matches?.("#systemLastError")) {
      meta.textContent = text;
    }
  }
}

function renderSnapshot() {
  const snapshot = getViewingSnapshot();
  if (!snapshot || !Array.isArray(snapshot.rows)) {
    const loadingSelectedSnapshot =
      Boolean(state.selectedSnapshotId) && state.snapshotDetailLoadingId === state.selectedSnapshotId;
    els.masterRowsBody.innerHTML = `<tr><td colspan="9" class="empty">${
      loadingSelectedSnapshot ? "正在加载所选月份持仓..." : "暂无自动抓取到的持仓表。"
    }</td></tr>`;
    els.latestSnapshotMeta.textContent = loadingSelectedSnapshot ? "正在加载历史月份详情" : "暂无抓取结果";
    return;
  }

  if (snapshot.rows.length === 0) {
    els.masterRowsBody.innerHTML = `<tr><td colspan="9" class="empty">暂无自动抓取到的持仓表。</td></tr>`;
    els.latestSnapshotMeta.textContent = "暂无抓取结果";
    return;
  }

  const sourceLabel = snapshot.source === "xueqiu" ? "雪球" : snapshot.source === "weibo" ? "微博" : snapshot.source;
  const monthLabel = monthLabelFromTitleOrDate(snapshot.title, snapshot.postedAt);
  const viewLabel = state.selectedSnapshotId ? "历史查看中" : "最新";
  const anomalyCount = getSnapshotAnomalyCount(snapshot.postId);
  const sourceStats = snapshot.rows.reduce(
    (acc, item) => {
      const source = String(item?.nameSource || "").trim().toLowerCase();
      if (source === "xueqiu") {
        acc.xueqiu += 1;
      } else if (source === "tencent") {
        acc.tencent += 1;
      } else {
        acc.local += 1;
      }
      return acc;
    },
    { xueqiu: 0, tencent: 0, local: 0 }
  );
  const sourceSummary = [
    sourceStats.xueqiu > 0 ? `雪球名 ${sourceStats.xueqiu}` : "",
    sourceStats.tencent > 0 ? `腾讯备份 ${sourceStats.tencent}` : "",
    sourceStats.local > 0 ? `本地纠错 ${sourceStats.local}` : ""
  ]
    .filter(Boolean)
    .join(" / ");
  els.latestSnapshotMeta.textContent = `${viewLabel} | ${monthLabel} | ${sourceLabel} | ${formatDateTime(
    snapshot.postedAt
  )} | ${snapshot.rows.length} 行${anomalyCount > 0 ? ` | 复核 ${anomalyCount} 行` : ""}${
    sourceSummary ? ` | ${sourceSummary}` : ""
  }`;

  const rows = snapshot.rows
    .map((item) => {
      const diagnostics = getRowDiagnostics(snapshot, item);
      const qtyRaw = Number.isFinite(Number(item.holdingQty))
        ? Number(item.holdingQty)
        : Number(item.changeQty) || 0;
      const costRaw = Number.isFinite(Number(item.referenceCost))
        ? Number(item.referenceCost)
        : Number(item.latestCost);
      const latestPriceRaw = Number.isFinite(Number(item.latestPrice)) ? Number(item.latestPrice) : null;
      const marketValueRaw = Number.isFinite(Number(item.marketValue)) ? Number(item.marketValue) : null;
      const floatingPnlRaw = Number.isFinite(Number(item.floatingPnl)) ? Number(item.floatingPnl) : null;
      const pnlPctRaw = Number.isFinite(Number(item.pnlPct)) ? Number(item.pnlPct) : null;

      const pnlClass = floatingPnlRaw > 0 ? "pos" : floatingPnlRaw < 0 ? "neg" : "";
      const pctClass = pnlPctRaw > 0 ? "pos" : pnlPctRaw < 0 ? "neg" : "";
      const qtyText = formatNumber(qtyRaw, 0);
      const nameBadge = diagnostics
        ? `<span class="tag ${diagnostics.level === "warn" ? "tag-warn" : "tag-fix"}">${
            diagnostics.level === "warn" ? "复核" : "已修正"
          }</span>`
        : "";
      const nameSourceLabel = escapeHtml(formatNameSourceLabel(item.nameSource));
      const nameSourceClass = formatNameSourceTagClass(item.nameSource);
      const symbol = escapeHtml(item.symbol || "-");
      const name = escapeHtml(item.name || "-");

      return `
        <tr class="${diagnostics ? "row-anomaly" : ""}">
          <td class="mono">${symbol}</td>
          <td>${name} ${nameBadge}</td>
          <td><span class="tag ${nameSourceClass}">${nameSourceLabel}</span></td>
          <td class="mono">${escapeHtml(qtyText)}</td>
          <td class="mono">¥ ${escapeHtml(formatNumber(costRaw, 3))}</td>
          <td class="mono">${latestPriceRaw === null ? "-" : `¥ ${escapeHtml(formatNumber(latestPriceRaw, 3))}`}</td>
          <td class="mono">${marketValueRaw === null ? "-" : `¥ ${escapeHtml(formatNumber(marketValueRaw, 3))}`}</td>
          <td class="mono ${pnlClass}">${floatingPnlRaw === null ? "-" : `¥ ${escapeHtml(formatNumber(floatingPnlRaw, 3))}`}</td>
          <td class="mono ${pctClass}">${pnlPctRaw === null ? "-" : escapeHtml(formatNumber(pnlPctRaw, 3))}</td>
        </tr>
      `;
    })
    .join("");

  els.masterRowsBody.innerHTML = rows;
}

function renderLogs() {
  const logs = state.autoTracking?.recentLogs || [];
  if (!logs.length) {
    els.syncLogsBody.innerHTML = `<tr><td colspan="3" class="empty">暂无日志。</td></tr>`;
    return;
  }

  const rows = logs
    .slice(0, 20)
    .map((log) => {
      const level = String(log.level || "info").toUpperCase();
      const levelTagClass = level === "ERROR" ? "tag-error" : level === "WARN" ? "tag-warn" : "tag-idle";
      return `
        <tr>
          <td class="mono">${formatDateTime(log.createdAt)}</td>
          <td><span class="tag ${levelTagClass}">${escapeHtml(level)}</span></td>
          <td>
            <div class="cell-stack">
              <strong class="cell-title" title="${escapeHtml(log.message || "-")}">${escapeHtml(log.message || "-")}</strong>
            </div>
          </td>
        </tr>
      `;
    })
    .join("");

  els.syncLogsBody.innerHTML = rows;
}

function renderCatalog() {
  if (!els.catalogBody || !els.catalogMeta) {
    return;
  }

  if (!state.catalogPosts.length) {
    els.catalogMeta.textContent = "未加载";
    els.catalogBody.innerHTML = `<tr><td colspan="6" class="empty">尚未抓取目录，点击“抓取全部月份目录”。</td></tr>`;
    if (els.catalogCheckAll) {
      els.catalogCheckAll.checked = false;
      els.catalogCheckAll.indeterminate = false;
      els.catalogCheckAll.disabled = true;
    }
    return;
  }

  const importedPostIds = new Set(
    getSnapshotHistoryList().map((item) => normalizePostId(item.postId)).filter(Boolean)
  );
  const rows = state.catalogPosts
    .map((item) => {
      const postId = normalizePostId(item.postId);
      const hasPostId = Boolean(postId);
      const imported = hasPostId && (Boolean(item.imported) || importedPostIds.has(postId));
      const statusText = !hasPostId ? "缺少帖子ID" : imported ? "已导入" : item.processed ? "已处理" : "未导入";
      const statusTagClass = imported ? "tag-fix" : !hasPostId ? "tag-error" : item.processed ? "tag-warn" : "tag-idle";
      const monthLabel = monthLabelFromTitleOrDate(item.title, item.postedAt);
      const safeTitle = escapeHtml(item.title || "(无标题)");
      const checkboxAttrs = hasPostId ? `data-catalog-id="${escapeHtml(postId)}"` : "disabled";
      const safeLink = buildExternalLinkHtml(item.link, "打开原帖");

      return `
        <tr>
          <td><input type="checkbox" ${checkboxAttrs} /></td>
          <td class="mono">${escapeHtml(monthLabel)}</td>
          <td>
            <div class="cell-stack">
              <strong class="cell-title" title="${safeTitle}">${safeTitle}</strong>
              <span class="cell-note" title="${hasPostId ? escapeHtml(postId) : "帖子ID 缺失"}">${hasPostId ? escapeHtml(postId) : "帖子ID 缺失"}</span>
            </div>
          </td>
          <td class="mono">${formatDateTime(item.postedAt)}</td>
          <td><span class="tag ${statusTagClass}">${escapeHtml(statusText)}</span></td>
          <td>${safeLink}</td>
        </tr>
      `;
    })
    .join("");

  els.catalogBody.innerHTML = rows;
  syncCatalogCheckAllState();
  updateCatalogMeta();
}

function renderSnapshotHistory() {
  if (!els.snapshotHistoryBody || !els.snapshotHistoryMeta) {
    return;
  }

  const list = getSnapshotHistoryList();
  if (list.length === 0) {
    els.snapshotHistoryMeta.textContent = "暂无";
    els.snapshotHistoryBody.innerHTML = `<tr><td colspan="5" class="empty">暂无历史快照。</td></tr>`;
    return;
  }

  const rows = list
    .map((item) => {
      const active = state.selectedSnapshotId === item.id;
      const monthLabel = monthLabelFromTitleOrDate(item.title, item.postedAt);
      const anomalyCount = getSnapshotAnomalyCount(item.postId);
      const safeTitle = escapeHtml(item.title || "-");
      const safeId = escapeHtml(item.id);
      const actionButtonClass = active ? "inline-btn inline-btn-primary" : "inline-btn";
      return `
        <tr class="${active ? "row-selected" : ""}">
          <td class="mono">${escapeHtml(monthLabel)}</td>
          <td>
            <div class="cell-stack">
              <strong class="cell-title" title="${safeTitle}">${safeTitle}</strong>
              <span class="cell-note">
                ${anomalyCount > 0 ? `<span class="tag tag-warn">复核 ${escapeHtml(anomalyCount)}</span>` : `<span class="tag tag-idle">正常</span>`}
              </span>
            </div>
          </td>
          <td class="mono">${formatDateTime(item.postedAt)}</td>
          <td class="mono">${escapeHtml(getSnapshotRowCount(item))}</td>
          <td><button class="${actionButtonClass}" data-view-snapshot="${safeId}">${active ? "当前查看" : "查看"}</button></td>
        </tr>
      `;
    })
    .join("");

  els.snapshotHistoryBody.innerHTML = rows;
  const anomalySnapshots = list.filter((item) => getSnapshotAnomalyCount(item.postId) > 0).length;
  els.snapshotHistoryMeta.textContent = `已导入 ${list.length} 条月份快照${anomalySnapshots > 0 ? ` / 需复核 ${anomalySnapshots} 条` : ""}`;
}

function renderAnomalies() {
  if (!els.anomalyMeta || !els.anomalyRowsBody) {
    return;
  }

  const report = state.anomalyReport;
  const snapshots = Array.isArray(report?.snapshots) ? report.snapshots : [];
  if (state.anomalyLoading && snapshots.length === 0) {
    els.anomalyMeta.textContent = "正在加载异常清单...";
    els.anomalyRowsBody.innerHTML = `<tr><td colspan="5" class="empty">正在计算异常行，请稍候。</td></tr>`;
    return;
  }

  if (snapshots.length === 0) {
    els.anomalyMeta.textContent = "未发现需要复核或自动修正的快照行";
    els.anomalyRowsBody.innerHTML = `<tr><td colspan="5" class="empty">当前没有需要展示的异常行。</td></tr>`;
    return;
  }

  const summary = report.summary || {};
  els.anomalyMeta.textContent = `涉及 ${summary.snapshotCount || snapshots.length} 个快照 / ${summary.rowCount || 0} 行，自动修正 ${
    summary.changedRowCount || 0
  } 行，仍需复核 ${summary.issueRowCount || 0} 行`;

  const rows = snapshots
    .flatMap((snapshot) =>
      (snapshot.rows || []).map((row) => {
        const diagnostics = row.diagnostics || { fixes: [], issues: [], level: "fix" };
        const monthLabel = monthLabelFromTitleOrDate(snapshot.title, snapshot.postedAt);
        const title = escapeHtml(snapshot.title || "-");
        const security = `${escapeHtml(row.symbol || "-")} ${escapeHtml(row.name || "-")}`;
        const fixText =
          diagnostics.fixes.length > 0 ? diagnostics.fixes.map((item) => escapeHtml(item)).join("<br />") : "-";
        const issueText =
          diagnostics.issues.length > 0 ? diagnostics.issues.map((item) => escapeHtml(item)).join("<br />") : "-";
        const values = escapeHtml(formatSnapshotValueSummary(row) || "-");

        return `
          <tr class="${diagnostics.issues.length > 0 ? "row-anomaly" : ""}">
            <td class="mono">${monthLabel}</td>
            <td>
              <div class="cell-stack">
                <strong class="cell-title" title="${security}">${security}</strong>
                <span class="cell-note" title="${title}">${title}</span>
              </div>
            </td>
            <td>${fixText}</td>
            <td class="${diagnostics.issues.length > 0 ? "neg" : ""}">${issueText}</td>
            <td class="mono">${values}</td>
          </tr>
        `;
      })
    )
    .join("");

  els.anomalyRowsBody.innerHTML = rows;
}

function renderMonthlyMetrics() {
  if (!els.monthlyMetricMeta || !els.monthlyMetricBody) {
    return;
  }

  const list = Array.isArray(state.monthlyUpdates) ? state.monthlyUpdates : [];
  const dirtyIds = syncMonthlyMetricToolbarState();
  const dirtyIdSet = new Set(dirtyIds);
  const editingIdSet = state.monthlyMetricEditingIds;
  const visibleList = getVisibleMonthlyUpdates(list, dirtyIdSet);

  if (list.length === 0) {
    els.monthlyMetricMeta.textContent = "暂无";
    els.monthlyMetricBody.innerHTML = `<tr><td colspan="7" class="empty">暂无可校正的月度指标。</td></tr>`;
    return;
  }

  const manualCount = list.filter((item) => hasManualMetrics(item.manualMetrics)).length;
  els.monthlyMetricMeta.textContent = `当前显示 ${visibleList.length} / 共 ${list.length} 个月份，人工校正 ${manualCount} 条`;

  if (visibleList.length === 0) {
    els.monthlyMetricBody.innerHTML = `<tr><td colspan="7" class="empty">${getMonthlyMetricEmptyMessage()}</td></tr>`;
    return;
  }

  const rows = visibleList
    .map((item) => {
      const draft = getMonthlyMetricDraft(item);
      const manualMetrics = hasManualMetrics(item.manualMetrics) ? item.manualMetrics : null;
      const isEditing = editingIdSet.has(item.id);
      const isDirty = dirtyIdSet.has(item.id);
      const autoValues = {
        netValueWan: Number.isFinite(Number(item.netValue)) ? Number(item.netValue) / 10_000 : null,
        netIndex: Number.isFinite(Number(item.netIndex)) && Number(item.netIndex) > 0 ? Number(item.netIndex) : null,
        yearStartNetIndex:
          Number.isFinite(Number(item.yearStartNetIndex)) && Number(item.yearStartNetIndex) > 0
            ? Number(item.yearStartNetIndex)
            : null
      };
      let modeTag = `<span class="tag">自动</span>`;
      if (manualMetrics) {
        modeTag = `<span class="tag tag-fix">人工</span>`;
      }
      if (isEditing && !isDirty) {
        modeTag = `<span class="tag tag-warn">编辑中</span>`;
      }
      if (isDirty) {
        modeTag = `<span class="tag tag-edit">待保存</span>`;
      }
      const statusCopy = isDirty
        ? "本行存在未保存修改"
        : manualMetrics
          ? "当前以前台人工值为准"
          : "当前沿用自动抓取结果";
      const title = escapeHtml(item.title || "-");
      const sourceLabel = escapeHtml(formatSourceLabel(item.source));
      const link = item.link
        ? `<a class="cell-link" href="${item.link}" target="_blank" rel="noreferrer">查看原帖</a>`
        : "";
      const actionButtons = isEditing
        ? `
            <button class="inline-btn inline-btn-primary" type="button" data-save-monthly-metrics="${escapeHtml(item.id)}">保存</button>
            <button class="inline-btn" type="button" data-cancel-monthly-metrics="${escapeHtml(item.id)}">取消</button>
            <button class="inline-btn" type="button" data-clear-monthly-metrics="${escapeHtml(item.id)}">恢复自动</button>
          `
        : `
            <button class="inline-btn" type="button" data-edit-monthly-metrics="${escapeHtml(item.id)}">编辑</button>
            ${
              manualMetrics
                ? `<button class="inline-btn" type="button" data-clear-monthly-metrics="${escapeHtml(item.id)}">恢复自动</button>`
                : ""
            }
          `;

      return `
        <tr class="${[isDirty ? "row-dirty" : "", isEditing ? "row-editing" : ""].filter(Boolean).join(" ")}" data-monthly-update-id="${escapeHtml(item.id)}">
          <td class="mono">${escapeHtml(item.month || monthLabelByDate(item.postedAt))}</td>
          <td>
            <div class="cell-stack">
              <strong class="cell-title" title="${title}">${title}</strong>
              <span class="cell-note" title="${sourceLabel} | ${formatDateTime(item.postedAt)}">${sourceLabel} | ${formatDateTime(item.postedAt)} ${link}</span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor ${isEditing ? "is-editing" : ""}">
              <span class="metric-editor-label">人工值</span>
              <input
                class="metric-input mono"
                data-metric-field="netValueWan"
                data-update-id="${escapeHtml(item.id)}"
                type="number"
                min="0"
                step="0.01"
                value="${escapeHtml(normalizeDraftFieldValue(draft?.netValueWan))}"
                placeholder="输入人工值"
                ${isEditing ? "" : "disabled"}
              />
              <span class="metric-hint">
                <span class="metric-hint-label">自动值</span>
                <strong class="metric-hint-value">${escapeHtml(formatMetricHint(autoValues.netValueWan, 2))}</strong>
              </span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor ${isEditing ? "is-editing" : ""}">
              <span class="metric-editor-label">人工值</span>
              <input
                class="metric-input mono"
                data-metric-field="netIndex"
                data-update-id="${escapeHtml(item.id)}"
                type="number"
                min="0"
                step="0.0001"
                value="${escapeHtml(normalizeDraftFieldValue(draft?.netIndex))}"
                placeholder="输入人工值"
                ${isEditing ? "" : "disabled"}
              />
              <span class="metric-hint">
                <span class="metric-hint-label">自动值</span>
                <strong class="metric-hint-value">${escapeHtml(formatMetricHint(autoValues.netIndex, 4))}</strong>
              </span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor ${isEditing ? "is-editing" : ""}">
              <span class="metric-editor-label">人工值</span>
              <input
                class="metric-input mono"
                data-metric-field="yearStartNetIndex"
                data-update-id="${escapeHtml(item.id)}"
                type="number"
                min="0"
                step="0.0001"
                value="${escapeHtml(normalizeDraftFieldValue(draft?.yearStartNetIndex))}"
                placeholder="输入人工值"
                ${isEditing ? "" : "disabled"}
              />
              <span class="metric-hint">
                <span class="metric-hint-label">自动值</span>
                <strong class="metric-hint-value">${escapeHtml(formatMetricHint(autoValues.yearStartNetIndex, 4))}</strong>
              </span>
            </div>
          </td>
          <td>
            <div class="metric-status-cell">
              ${modeTag}
              <span class="metric-status-copy">${escapeHtml(statusCopy)}</span>
            </div>
          </td>
          <td>
            <div class="metric-actions ${isEditing ? "is-editing" : ""}">
              ${actionButtons}
            </div>
          </td>
        </tr>
      `;
    })
    .join("");

  els.monthlyMetricBody.innerHTML = rows;
}

function formatJobStatusText(job) {
  if (!job) {
    return "当前没有运行任务";
  }
  if (job.status === "running") {
    return `${job.label || "任务"} · 执行中`;
  }
  if (job.status === "failed") {
    return `${job.label || "任务"} · 失败`;
  }
  if (job.skipped) {
    return `${job.label || "任务"} · 已跳过`;
  }
  return `${job.label || "任务"} · 已完成`;
}

function renderJobStatus(overview) {
  const running = overview?.running || null;
  const recent = Array.isArray(overview?.recent) ? overview.recent[0] : null;
  const job = running || recent || null;
  const progress = Math.max(0, Math.min(100, Number(job?.progress) || 0));

  if (els.jobStatusTitle) {
    els.jobStatusTitle.textContent = formatJobStatusText(job);
  }
  if (els.jobStatusSummary) {
    els.jobStatusSummary.textContent = job?.message || "点击抓取、回溯或保活后，这里会显示执行阶段和结果。";
  }
  if (els.jobStatusProgress) {
    els.jobStatusProgress.setAttribute("aria-valuenow", String(progress));
    const bar = els.jobStatusProgress.querySelector("span");
    if (bar) {
      bar.style.width = `${progress}%`;
    }
  }
  if (els.jobStatusMeta) {
    els.jobStatusMeta.textContent = job
      ? `阶段：${job.stage || "-"} · 状态：${job.status || "-"}`
      : "等待任务";
  }
  if (els.jobStatusCard) {
    els.jobStatusCard.classList.toggle("is-running", Boolean(running));
    els.jobStatusCard.classList.toggle("is-failed", job?.status === "failed");
  }
}

async function loadJobOverview({ silent = false } = {}) {
  try {
    const overview = await request("/api/jobs/overview");
    state.jobOverview = overview;
    renderJobStatus(overview);
    if (overview?.running) {
      startJobPolling();
    } else {
      stopJobPolling();
    }
  } catch (error) {
    if (!silent) {
      setStatus(`任务状态读取失败：${error.message}`, "error");
    }
  }
}

function startJobPolling() {
  if (state.jobPollTimer) {
    return;
  }
  state.jobPollTimer = window.setInterval(() => {
    loadJobOverview({ silent: true });
  }, 1500);
}

function stopJobPolling() {
  if (!state.jobPollTimer) {
    return;
  }
  window.clearInterval(state.jobPollTimer);
  state.jobPollTimer = null;
}

function renderAll() {
  renderForm();
  renderSystemStatus();
  renderWorkContext();
  renderSnapshot();
  renderLogs();
  renderCatalog();
  renderSnapshotHistory();
  renderAnomalies();
  renderMonthlyMetrics();
}

async function ensureSnapshotDetail(snapshotId, loadVersion = state.loadVersion) {
  const targetId = String(snapshotId || "").trim();
  if (!targetId) {
    return;
  }

  if (state.snapshotCache.has(targetId)) {
    return;
  }

  if (!getSnapshotHistoryList().some((item) => String(item?.id || "").trim() === targetId)) {
    return;
  }

  state.snapshotDetailLoadingId = targetId;
  if (state.selectedSnapshotId === targetId) {
    renderSnapshot();
  }

  try {
    const data = await request(`/api/master-snapshots/${encodeURIComponent(targetId)}`);
    if (loadVersion !== state.loadVersion) {
      return;
    }

    if (data?.snapshot?.id) {
      state.snapshotCache.set(String(data.snapshot.id), data.snapshot);
    }
  } finally {
    if (loadVersion === state.loadVersion && state.snapshotDetailLoadingId === targetId) {
      state.snapshotDetailLoadingId = null;
    }
    if (state.selectedSnapshotId === targetId) {
      renderSnapshot();
    }
  }
}

async function loadAnomalyReport(loadVersion = state.loadVersion) {
  state.anomalyLoading = true;
  renderAnomalies();

  try {
    const report = await request("/api/auto-tracking/anomalies");
    if (loadVersion !== state.loadVersion) {
      return;
    }

    state.anomalyReport = report || { summary: {}, snapshots: [] };
    rebuildAnomalyIndex();
  } catch (_error) {
    if (loadVersion !== state.loadVersion) {
      return;
    }
    state.anomalyReport = state.anomalyReport || { summary: {}, snapshots: [] };
  } finally {
    if (loadVersion === state.loadVersion) {
      state.anomalyLoading = false;
      renderSnapshot();
      renderSnapshotHistory();
      renderAnomalies();
    }
  }
}

async function loadData() {
  const nextLoadVersion = state.loadVersion + 1;
  state.loadVersion = nextLoadVersion;
  const bootstrap = await request("/api/auto-tracking/bootstrap?limit=240");

  state.autoTracking = bootstrap.autoTracking || null;
  state.latestSnapshot = bootstrap.latestSnapshot || null;
  state.snapshots = Array.isArray(bootstrap.snapshotHistory)
    ? bootstrap.snapshotHistory
    : Array.isArray(bootstrap.snapshots)
      ? bootstrap.snapshots
      : [];
  syncSnapshotCache();
  state.monthlyUpdates = Array.isArray(bootstrap.monthlyUpdates?.updates) ? bootstrap.monthlyUpdates.updates : [];
  reconcileMonthlyMetricUiState();
  state.anomalyReport = bootstrap.anomalyReport || { summary: {}, snapshots: [] };
  rebuildAnomalyIndex();
  state.anomalyLoading = !bootstrap.anomalyReport;

  if (state.selectedSnapshotId && !getSnapshotHistoryList().some((item) => item.id === state.selectedSnapshotId)) {
    state.selectedSnapshotId = null;
  }

  renderAll();

  if (state.selectedSnapshotId) {
    void ensureSnapshotDetail(state.selectedSnapshotId, nextLoadVersion);
  }

  if (!bootstrap.anomalyReport) {
    void loadAnomalyReport(nextLoadVersion);
  }
}

async function fetchCatalog({ silent = false } = {}) {
  const pagesValue = Number(els.autoConfigForm?.elements?.backfillMaxPages?.value);
  const pageSizeValue = Number(els.autoConfigForm?.elements?.backfillPageSize?.value);

  const data = await request("/api/auto-tracking/catalog", {
    method: "POST",
    body: JSON.stringify({
      pages: Number.isFinite(pagesValue) ? pagesValue : undefined,
      pageSize: Number.isFinite(pageSizeValue) ? pageSizeValue : undefined
    })
  });

  state.catalogPosts = Array.isArray(data.posts) ? data.posts : [];
  renderCatalog();

  if (!silent) {
    setStatus(`目录抓取完成：${state.catalogPosts.length} 条月份帖子`, "ok");
  }
}

async function handleSaveConfig(event) {
  event.preventDefault();

  const formData = new FormData(els.autoConfigForm);
  const payload = Object.fromEntries(formData.entries());

  payload.enabled = String(payload.enabled) === "true";
  payload.smartScheduleEnabled = String(payload.smartScheduleEnabled) === "true";
  payload.skipStartupOutsideWindow = String(payload.skipStartupOutsideWindow) === "true";
  payload.cookieKeepAliveEnabled = String(payload.cookieKeepAliveEnabled) === "true";
  payload.ocrEnabled = String(payload.ocrEnabled) === "true";
  payload.ocrProvider = String(payload.ocrProvider || "auto").trim();
  payload.intervalMinutes = Number(payload.intervalMinutes);
  payload.monthEndWindowDays = Number(payload.monthEndWindowDays);
  payload.offWindowIntervalHours = Number(payload.offWindowIntervalHours);
  payload.cookieKeepAliveIntervalHours = Number(payload.cookieKeepAliveIntervalHours);
  payload.maxPostsPerSource = Number(payload.maxPostsPerSource);
  payload.ocrMaxImagesPerPost = Number(payload.ocrMaxImagesPerPost);
  payload.backfillMaxPages = Number(payload.backfillMaxPages);
  payload.backfillPageSize = Number(payload.backfillPageSize);

  const xueqiuCookie = String(payload.xueqiuCookie || "").trim();
  const weiboCookie = String(payload.weiboCookie || "").trim();
  const qwenApiKey = String(payload.qwenApiKey || "").trim();

  if (xueqiuCookie) {
    payload.xueqiuCookie = xueqiuCookie;
  } else {
    delete payload.xueqiuCookie;
  }

  if (weiboCookie) {
    payload.weiboCookie = weiboCookie;
  } else {
    delete payload.weiboCookie;
  }

  if (qwenApiKey) {
    payload.qwenApiKey = qwenApiKey;
  } else {
    delete payload.qwenApiKey;
  }

  try {
    setActionBusy(true);
    setStatus("正在保存配置...", "info");
    await request("/api/auto-tracking/config", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    await loadData();
    setStatus("配置保存成功", "ok");
  } catch (error) {
    setStatus(`保存失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleRunCookieKeepAlive() {
  try {
    setActionBusy(true);
    setStatus("正在执行 Cookie 保活...", "info");
    const res = await request("/api/auto-tracking/cookie-keepalive", {
      method: "POST"
    });

    await loadData();
    await loadJobOverview({ silent: true });
    const status = resolveCookieKeepAliveResultStatus(res?.result);
    setStatus(status.text, status.level);
  } catch (error) {
    setStatus(`Cookie 保活失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleRunNow() {
  try {
    setActionBusy(true);
    setStatus("正在抓取...", "info");
    const res = await request("/api/auto-tracking/run", {
      method: "POST"
    });

    await loadData();
    await loadJobOverview({ silent: true });
    const status = resolveAutoTrackingResultStatus(res?.result, "抓取");
    setStatus(status.text, status.level);
  } catch (error) {
    setStatus(`抓取失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleRunBackfill() {
  try {
    const pagesValue = Number(els.autoConfigForm?.elements?.backfillMaxPages?.value);
    const pageSizeValue = Number(els.autoConfigForm?.elements?.backfillPageSize?.value);

    setActionBusy(true);
    setStatus("正在回溯历史标题帖子并识别截图...", "info");
    const res = await request("/api/auto-tracking/backfill", {
      method: "POST",
      body: JSON.stringify({
        pages: Number.isFinite(pagesValue) ? pagesValue : undefined,
        pageSize: Number.isFinite(pageSizeValue) ? pageSizeValue : undefined
      })
    });

    await loadData();
    await loadJobOverview({ silent: true });
    const status = resolveAutoTrackingResultStatus(res?.result, "回溯");
    setStatus(status.text, status.level);
  } catch (error) {
    setStatus(`回溯失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleLoadCatalog() {
  try {
    setActionBusy(true);
    setStatus("正在抓取全部月份目录...", "info");
    await fetchCatalog({ silent: true });
    setStatus(`目录抓取完成：${state.catalogPosts.length} 条月份帖子`, "ok");
  } catch (error) {
    setStatus(`目录抓取失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleImportSelected() {
  try {
    const checked = Array.from(els.catalogBody?.querySelectorAll("input[data-catalog-id]:checked") || []);
    const postIds = [
      ...new Set(checked.map((item) => normalizePostId(item.dataset.catalogId)).filter(Boolean))
    ];

    if (postIds.length === 0) {
      setStatus("请先勾选要导入的月份帖子", "err");
      return;
    }

    const pagesValue = Number(els.autoConfigForm?.elements?.backfillMaxPages?.value);
    const pageSizeValue = Number(els.autoConfigForm?.elements?.backfillPageSize?.value);

    setActionBusy(true);
    setStatus(`正在导入选中月份（${postIds.length} 条）...`, "info");
    const res = await request("/api/auto-tracking/import-selected", {
      method: "POST",
      body: JSON.stringify({
        postIds,
        pages: Number.isFinite(pagesValue) ? pagesValue : undefined,
        pageSize: Number.isFinite(pageSizeValue) ? pageSizeValue : undefined
      })
    });

    await loadData();
    await loadJobOverview({ silent: true });
    if (state.catalogPosts.length > 0) {
      await fetchCatalog({ silent: true });
    }

    const status = resolveAutoTrackingResultStatus(res?.result, "导入");
    setStatus(status.text, status.level);
  } catch (error) {
    setStatus(`导入失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

async function handleRecalculateSnapshots() {
  try {
    setActionBusy(true);
    setStatus("正在重算历史快照字段并生成异常清单...", "info");
    const res = await request("/api/auto-tracking/recalculate-snapshots", {
      method: "POST"
    });
    await loadData();
    await loadJobOverview({ silent: true });
    const changedSnapshots = Number(res?.summary?.changedSnapshotCount) || 0;
    const changedRows = Number(res?.summary?.changedRowCount) || 0;
    const issueRows = Number(res?.summary?.issueRowCount) || 0;
    setStatus(
      `重算完成：更新 ${changedSnapshots} 个快照，自动修正 ${changedRows} 行，仍需复核 ${issueRows} 行`,
      "ok"
    );
  } catch (error) {
    setStatus(`重算失败: ${error.message}`, "err");
  } finally {
    setActionBusy(false);
  }
}

function handleCatalogCheckAllChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }

  const shouldCheck = target.checked;
  getCatalogCheckboxes()
    .filter((input) => !input.disabled)
    .forEach((input) => {
      input.checked = shouldCheck;
    });

  syncCatalogCheckAllState();
  updateCatalogMeta();
}

function handleCatalogBodyChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }

  if (!target.matches("input[data-catalog-id]")) {
    return;
  }

  syncCatalogCheckAllState();
  updateCatalogMeta();
}

function handleSnapshotTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const button = target.closest("[data-view-snapshot]");
  if (!button) {
    return;
  }

  const id = String(button.getAttribute("data-view-snapshot") || "").trim();
  if (!id) {
    return;
  }

  state.selectedSnapshotId = id;
  renderSnapshot();
  renderSnapshotHistory();
  void ensureSnapshotDetail(id);
}

function handleViewLatestSnapshot() {
  state.selectedSnapshotId = null;
  renderSnapshot();
  renderSnapshotHistory();
}

function startMonthlyMetricEdit(updateId) {
  const update = getMonthlyUpdateById(updateId);
  if (!update) {
    return;
  }

  setMonthlyMetricDraft(updateId, getMonthlyMetricDraft(update));
  state.monthlyMetricEditingIds.add(updateId);
  renderMonthlyMetrics();
}

function cancelMonthlyMetricEdit(updateId) {
  clearMonthlyMetricDraft(updateId);
  state.monthlyMetricEditingIds.delete(updateId);
  renderMonthlyMetrics();
}

function updateMonthlyMetricDraftFromInput(target) {
  if (!(target instanceof HTMLInputElement)) {
    return;
  }

  const updateId = String(target.dataset.updateId || "").trim();
  const field = String(target.dataset.metricField || "").trim();
  if (!updateId || !field) {
    return;
  }

  const update = getMonthlyUpdateById(updateId);
  if (!update) {
    return;
  }

  const draft = {
    ...getMonthlyMetricDraft(update),
    [field]: normalizeDraftFieldValue(target.value)
  };

  setMonthlyMetricDraft(updateId, draft);
}

function buildMonthlyMetricPayload(updateId) {
  const update = getMonthlyUpdateById(updateId);
  const draft = update ? getMonthlyMetricDraft(update) : null;

  return {
    netValueWan: normalizeDraftFieldValue(draft?.netValueWan),
    netIndex: normalizeDraftFieldValue(draft?.netIndex),
    yearStartNetIndex: normalizeDraftFieldValue(draft?.yearStartNetIndex)
  };
}

async function patchMonthlyMetrics(updateId, payload) {
  return request(`/api/monthly-updates/${encodeURIComponent(updateId)}`, {
    method: "PATCH",
    body: JSON.stringify(payload)
  });
}

async function saveMonthlyMetrics(updateId) {
  const update = getMonthlyUpdateById(updateId);
  if (!update) {
    setStatus("未找到对应的月度记录", "err");
    return;
  }

  const payload = buildMonthlyMetricPayload(updateId);
  const isDirty = isMonthlyMetricDraftDirty(update, payload);
  if (!isDirty) {
    state.monthlyMetricEditingIds.delete(updateId);
    clearMonthlyMetricDraft(updateId);
    renderMonthlyMetrics();
    setStatus(`未检测到 ${update.month} 的修改`, "ok");
    return;
  }

  try {
    setStatus(`正在保存 ${update.month} 的月度指标...`, "info");
    await patchMonthlyMetrics(updateId, payload);
    state.monthlyMetricEditingIds.delete(updateId);
    clearMonthlyMetricDraft(updateId);
    await loadData();
    const savedAllBlank = !payload.netValueWan && !payload.netIndex && !payload.yearStartNetIndex;
    setStatus(savedAllBlank ? `已恢复 ${update.month} 的自动指标` : `已保存 ${update.month} 的月度指标`, "ok");
  } catch (error) {
    setStatus(`保存月度指标失败: ${error.message}`, "err");
  }
}

async function saveAllMonthlyMetrics() {
  const dirtyIds = getDirtyMonthlyMetricIds();
  if (dirtyIds.length === 0) {
    setStatus("当前没有待保存的月度指标修改", "ok");
    return;
  }

  try {
    setStatus(`正在批量保存 ${dirtyIds.length} 条月度指标...`, "info");
    for (const updateId of dirtyIds) {
      await patchMonthlyMetrics(updateId, buildMonthlyMetricPayload(updateId));
      state.monthlyMetricEditingIds.delete(updateId);
      clearMonthlyMetricDraft(updateId);
    }
    await loadData();
    setStatus(`已批量保存 ${dirtyIds.length} 条月度指标`, "ok");
  } catch (error) {
    setStatus(`批量保存失败: ${error.message}`, "err");
  }
}

function discardAllMonthlyMetricEdits() {
  state.monthlyMetricDrafts.clear();
  state.monthlyMetricEditingIds.clear();
  renderMonthlyMetrics();
  setStatus("已放弃全部未保存的月度指标编辑", "ok");
}

function setMonthlyMetricFilter(filter) {
  const nextFilter = String(filter || "").trim();
  if (!["all", "dirty", "manual"].includes(nextFilter)) {
    return;
  }

  state.monthlyMetricFilter = nextFilter;
  renderMonthlyMetrics();
}

async function clearMonthlyMetrics(updateId) {
  const update = getMonthlyUpdateById(updateId);
  if (!update) {
    setStatus("未找到对应的月度记录", "err");
    return;
  }

  try {
    setStatus(`正在恢复 ${update.month} 的自动指标...`, "info");
    await patchMonthlyMetrics(updateId, {
      clearManualMetrics: true
    });
    state.monthlyMetricEditingIds.delete(updateId);
    clearMonthlyMetricDraft(updateId);
    await loadData();
    setStatus(`已恢复 ${update.month} 的自动指标`, "ok");
  } catch (error) {
    setStatus(`恢复自动指标失败: ${error.message}`, "err");
  }
}

function handleMonthlyMetricTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const editButton = target.closest("[data-edit-monthly-metrics]");
  if (editButton) {
    const updateId = String(editButton.getAttribute("data-edit-monthly-metrics") || "").trim();
    if (updateId) {
      startMonthlyMetricEdit(updateId);
    }
    return;
  }

  const saveButton = target.closest("[data-save-monthly-metrics]");
  if (saveButton) {
    const updateId = String(saveButton.getAttribute("data-save-monthly-metrics") || "").trim();
    if (updateId) {
      void saveMonthlyMetrics(updateId);
    }
    return;
  }

  const cancelButton = target.closest("[data-cancel-monthly-metrics]");
  if (cancelButton) {
    const updateId = String(cancelButton.getAttribute("data-cancel-monthly-metrics") || "").trim();
    if (updateId) {
      cancelMonthlyMetricEdit(updateId);
    }
    return;
  }

  const clearButton = target.closest("[data-clear-monthly-metrics]");
  if (!clearButton) {
    return;
  }

  const updateId = String(clearButton.getAttribute("data-clear-monthly-metrics") || "").trim();
  if (updateId) {
    void clearMonthlyMetrics(updateId);
  }
}

function handleMonthlyMetricFilterClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const button = target.closest("[data-metric-filter]");
  if (!button) {
    return;
  }

  const filter = String(button.getAttribute("data-metric-filter") || "").trim();
  if (filter) {
    setMonthlyMetricFilter(filter);
  }
}

function bindEvents() {
  for (const button of document.querySelectorAll("[data-admin-section]")) {
    button.addEventListener("click", () => {
      setAdminSection(button.getAttribute("data-admin-section") || "overview");
    });
  }

  if (els.autoConfigForm) {
    els.autoConfigForm.addEventListener("submit", handleSaveConfig);
  }

  if (els.runAutoSyncBtn) {
    els.runAutoSyncBtn.addEventListener("click", handleRunNow);
  }

  if (els.runCookieKeepAliveBtn) {
    els.runCookieKeepAliveBtn.addEventListener("click", handleRunCookieKeepAlive);
  }

  if (els.runBackfillBtn) {
    els.runBackfillBtn.addEventListener("click", handleRunBackfill);
  }

  if (els.loadCatalogBtn) {
    els.loadCatalogBtn.addEventListener("click", handleLoadCatalog);
  }

  if (els.importSelectedBtn) {
    els.importSelectedBtn.addEventListener("click", handleImportSelected);
  }

  if (els.recalculateSnapshotsBtn) {
    els.recalculateSnapshotsBtn.addEventListener("click", handleRecalculateSnapshots);
  }

  if (els.catalogCheckAll) {
    els.catalogCheckAll.addEventListener("change", handleCatalogCheckAllChange);
  }

  if (els.catalogBody) {
    els.catalogBody.addEventListener("change", handleCatalogBodyChange);
  }

  if (els.snapshotHistoryBody) {
    els.snapshotHistoryBody.addEventListener("click", handleSnapshotTableClick);
  }

  if (els.viewLatestBtn) {
    els.viewLatestBtn.addEventListener("click", handleViewLatestSnapshot);
  }

  if (els.monthlyMetricBody) {
    els.monthlyMetricBody.addEventListener("click", handleMonthlyMetricTableClick);
    els.monthlyMetricBody.addEventListener("input", (event) => {
      updateMonthlyMetricDraftFromInput(event.target);
      syncMonthlyMetricToolbarState();
    });
    els.monthlyMetricBody.addEventListener("change", (event) => {
      updateMonthlyMetricDraftFromInput(event.target);
      renderMonthlyMetrics();
    });
  }

  if (els.saveAllMetricsBtn) {
    els.saveAllMetricsBtn.addEventListener("click", () => {
      void saveAllMonthlyMetrics();
    });
  }

  if (els.discardAllMetricsBtn) {
    els.discardAllMetricsBtn.addEventListener("click", discardAllMonthlyMetricEdits);
  }

  if (els.monthlyMetricFilters) {
    els.monthlyMetricFilters.addEventListener("click", handleMonthlyMetricFilterClick);
  }
}

async function bootstrap() {
  bindEvents();
  initAdminSectionFromUrl();
  try {
    await loadData();
    await loadJobOverview({ silent: true });
  } catch (error) {
    setStatus(`加载失败: ${error.message}`, "err");
  }
}

bootstrap();
