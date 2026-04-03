const state = {
  autoTracking: null,
  latestSnapshot: null,
  snapshots: [],
  monthlyUpdates: [],
  monthlyMetricDrafts: new Map(),
  monthlyMetricEditingIds: new Set(),
  monthlyMetricFilter: "all",
  catalogPosts: [],
  selectedSnapshotId: null,
  anomalyReport: null,
  anomalyRowsByKey: new Map()
};

const els = {
  autoConfigForm: document.getElementById("autoConfigForm"),
  runAutoSyncBtn: document.getElementById("runAutoSyncBtn"),
  runBackfillBtn: document.getElementById("runBackfillBtn"),
  loadCatalogBtn: document.getElementById("loadCatalogBtn"),
  importSelectedBtn: document.getElementById("importSelectedBtn"),
  recalculateSnapshotsBtn: document.getElementById("recalculateSnapshotsBtn"),
  catalogCheckAll: document.getElementById("catalogCheckAll"),
  viewLatestBtn: document.getElementById("viewLatestBtn"),
  autoSyncText: document.getElementById("autoSyncText"),
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
  discardAllMetricsBtn: document.getElementById("discardAllMetricsBtn")
};

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
}

function getViewingSnapshot() {
  if (state.selectedSnapshotId) {
    const hit = state.snapshots.find((item) => item.id === state.selectedSnapshotId);
    if (hit) {
      return hit;
    }
  }
  return state.latestSnapshot;
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

  return dirtyIds;
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

  setValue("enabled", String(Boolean(config.enabled)));
  setValue("intervalMinutes", String(config.intervalMinutes || 180));
  setValue("maxPostsPerSource", String(config.maxPostsPerSource || 6));
  setValue("ocrEnabled", String(Boolean(config.ocrEnabled)));
  setValue("ocrProvider", String(config.ocrProvider || "auto"));
  setValue("ocrMaxImagesPerPost", String(config.ocrMaxImagesPerPost || 1));
  setValue("pinnedPostUrls", Array.isArray(config.pinnedPostUrls) ? config.pinnedPostUrls.join("\n") : "");
  setValue("xueqiuTitleRegex", String(config.xueqiuTitleRegex || ""));
  setValue("backfillMaxPages", String(config.backfillMaxPages || 36));
  setValue("backfillPageSize", String(config.backfillPageSize || 20));
  setValue("keywords", Array.isArray(config.keywords) ? config.keywords.join(",") : "");

  const runtime = state.autoTracking?.runtime || {};
  const pinnedCount = Array.isArray(config.pinnedPostUrls) ? config.pinnedPostUrls.length : 0;
  const regexText = config.xueqiuTitleRegex ? ` / 标题规则:${config.xueqiuTitleRegex}` : "";
  const ocrProviderText =
    config.ocrProvider === "qwen"
      ? "Qwen OCR 优先"
      : config.ocrProvider === "local"
        ? "本地 Tesseract"
        : "自动(Qwen优先)";
  const cookieText = `雪球Cookie:${config.hasXueqiuCookie ? "已配置" : "未配置"} / 微博Cookie:${
    config.hasWeiboCookie ? "已配置" : "未配置"
  } / QwenKey:${config.hasQwenApiKey ? "已配置" : "未配置"} / OCR:${ocrProviderText} / 置顶链接:${pinnedCount}条${regexText}`;
  const runText = runtime.lastRunAt ? `最近执行: ${formatDateTime(runtime.lastRunAt)}` : "尚未执行";
  const errText = runtime.lastError ? ` | 最近错误: ${runtime.lastError}` : "";
  setStatus(`${cookieText} | ${runText}${errText}`, runtime.lastError ? "err" : "ok");
}

function renderSnapshot() {
  const snapshot = getViewingSnapshot();
  if (!snapshot || !Array.isArray(snapshot.rows) || snapshot.rows.length === 0) {
    els.masterRowsBody.innerHTML = `<tr><td colspan="8" class="empty">暂无自动抓取到的持仓表。</td></tr>`;
    els.latestSnapshotMeta.textContent = "暂无抓取结果";
    return;
  }

  const sourceLabel = snapshot.source === "xueqiu" ? "雪球" : snapshot.source === "weibo" ? "微博" : snapshot.source;
  const monthLabel = monthLabelFromTitleOrDate(snapshot.title, snapshot.postedAt);
  const viewLabel = state.selectedSnapshotId ? "历史查看中" : "最新";
  const anomalyCount = getSnapshotAnomalyCount(snapshot.postId);
  els.latestSnapshotMeta.textContent = `${viewLabel} | ${monthLabel} | ${sourceLabel} | ${formatDateTime(
    snapshot.postedAt
  )} | ${snapshot.rows.length} 行${anomalyCount > 0 ? ` | 复核 ${anomalyCount} 行` : ""}`;

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

      return `
        <tr class="${diagnostics ? "row-anomaly" : ""}">
          <td class="mono">${item.symbol || "-"}</td>
          <td>${item.name || "-"} ${nameBadge}</td>
          <td class="mono">${qtyText}</td>
          <td class="mono">¥ ${formatNumber(costRaw, 3)}</td>
          <td class="mono">${latestPriceRaw === null ? "-" : `¥ ${formatNumber(latestPriceRaw, 3)}`}</td>
          <td class="mono">${marketValueRaw === null ? "-" : `¥ ${formatNumber(marketValueRaw, 3)}`}</td>
          <td class="mono ${pnlClass}">${floatingPnlRaw === null ? "-" : `¥ ${formatNumber(floatingPnlRaw, 3)}`}</td>
          <td class="mono ${pctClass}">${pnlPctRaw === null ? "-" : formatNumber(pnlPctRaw, 3)}</td>
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
      const levelClass = level === "ERROR" ? "neg" : level === "WARN" ? "" : "pos";
      return `
        <tr>
          <td>${formatDateTime(log.createdAt)}</td>
          <td class="mono ${levelClass}">${level}</td>
          <td>${log.message || "-"}</td>
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
    (state.snapshots || []).map((item) => normalizePostId(item.postId)).filter(Boolean)
  );
  const rows = state.catalogPosts
    .map((item) => {
      const postId = normalizePostId(item.postId);
      const hasPostId = Boolean(postId);
      const imported = hasPostId && (Boolean(item.imported) || importedPostIds.has(postId));
      const statusText = !hasPostId ? "缺少帖子ID" : imported ? "已导入" : item.processed ? "已处理" : "未导入";
      const statusClass = imported ? "pos" : !hasPostId ? "neg" : "";
      const monthLabel = monthLabelFromTitleOrDate(item.title, item.postedAt);
      const safeTitle = item.title || "(无标题)";
      const checkboxAttrs = hasPostId ? `data-catalog-id="${postId}"` : "disabled";

      return `
        <tr>
          <td><input type="checkbox" ${checkboxAttrs} /></td>
          <td class="mono">${monthLabel}</td>
          <td>${safeTitle}</td>
          <td>${formatDateTime(item.postedAt)}</td>
          <td class="${statusClass}">${statusText}</td>
          <td>${item.link ? `<a href="${item.link}" target="_blank" rel="noreferrer">打开</a>` : "-"}</td>
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

  const list = Array.isArray(state.snapshots) ? state.snapshots : [];
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
      return `
        <tr class="${active ? "row-selected" : ""}">
          <td class="mono">${monthLabel}</td>
          <td>${item.title || "-"}${anomalyCount > 0 ? ` <span class="tag tag-warn">复核 ${anomalyCount}</span>` : ""}</td>
          <td>${formatDateTime(item.postedAt)}</td>
          <td class="mono">${Array.isArray(item.rows) ? item.rows.length : 0}</td>
          <td><button class="inline-btn" data-view-snapshot="${item.id}">查看</button></td>
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
                <strong>${security}</strong>
                <span class="cell-note">${title}</span>
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
        <tr class="${isDirty ? "row-dirty" : ""}" data-monthly-update-id="${escapeHtml(item.id)}">
          <td class="mono">${escapeHtml(item.month || monthLabelByDate(item.postedAt))}</td>
          <td>
            <div class="cell-stack">
              <strong>${title}</strong>
              <span class="cell-note">${sourceLabel} | ${formatDateTime(item.postedAt)} ${link}</span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor">
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
              <span class="metric-hint">自动值：${escapeHtml(formatMetricHint(autoValues.netValueWan, 2))}</span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor">
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
              <span class="metric-hint">自动值：${escapeHtml(formatMetricHint(autoValues.netIndex, 4))}</span>
            </div>
          </td>
          <td class="${isEditing ? "cell-editing" : ""}">
            <div class="metric-editor">
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
              <span class="metric-hint">自动值：${escapeHtml(formatMetricHint(autoValues.yearStartNetIndex, 4))}</span>
            </div>
          </td>
          <td>${modeTag}</td>
          <td>
            <div class="metric-actions">
              ${actionButtons}
            </div>
          </td>
        </tr>
      `;
    })
    .join("");

  els.monthlyMetricBody.innerHTML = rows;
}

function renderAll() {
  renderForm();
  renderSnapshot();
  renderLogs();
  renderCatalog();
  renderSnapshotHistory();
  renderAnomalies();
  renderMonthlyMetrics();
}

async function loadData() {
  const [autoData, snapshotsData, anomalyData, monthlyData] = await Promise.all([
    request("/api/auto-tracking"),
    request("/api/master-snapshots?limit=240"),
    request("/api/auto-tracking/anomalies"),
    request("/api/monthly-updates")
  ]);

  state.autoTracking = autoData.autoTracking || null;
  state.latestSnapshot = autoData.latestSnapshot || null;
  state.snapshots = Array.isArray(snapshotsData.snapshots) ? snapshotsData.snapshots : [];
  state.monthlyUpdates = Array.isArray(monthlyData.updates) ? monthlyData.updates : [];
  reconcileMonthlyMetricUiState();
  state.anomalyReport = anomalyData || { summary: {}, snapshots: [] };
  state.anomalyRowsByKey = new Map();

  for (const snapshot of state.anomalyReport.snapshots || []) {
    for (const row of snapshot.rows || []) {
      if (!row.diagnostics) {
        continue;
      }
      state.anomalyRowsByKey.set(buildAnomalyKey(snapshot.postId, row.symbol), row.diagnostics);
    }
  }

  if (state.selectedSnapshotId && !state.snapshots.some((item) => item.id === state.selectedSnapshotId)) {
    state.selectedSnapshotId = null;
  }

  renderAll();
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
  payload.ocrEnabled = String(payload.ocrEnabled) === "true";
  payload.ocrProvider = String(payload.ocrProvider || "auto").trim();
  payload.intervalMinutes = Number(payload.intervalMinutes);
  payload.maxPostsPerSource = Number(payload.maxPostsPerSource);
  payload.ocrMaxImagesPerPost = Number(payload.ocrMaxImagesPerPost);
  payload.backfillMaxPages = Number(payload.backfillMaxPages);
  payload.backfillPageSize = Number(payload.backfillPageSize);

  const xueqiuCookie = String(payload.xueqiuCookie || "").trim();
  const weiboCookie = String(payload.weiboCookie || "").trim();
  const qwenApiKey = String(payload.qwenApiKey || "").trim();

  if (!xueqiuCookie) {
    delete payload.xueqiuCookie;
  }
  if (!weiboCookie) {
    delete payload.weiboCookie;
  }
  if (!qwenApiKey) {
    delete payload.qwenApiKey;
  }

  try {
    setStatus("正在保存配置...", "info");
    await request("/api/auto-tracking/config", {
      method: "POST",
      body: JSON.stringify(payload)
    });
    await loadData();
    setStatus("配置保存成功", "ok");
  } catch (error) {
    setStatus(`保存失败: ${error.message}`, "err");
  }
}

async function handleRunNow() {
  try {
    setStatus("正在抓取...", "info");
    const res = await request("/api/auto-tracking/run", {
      method: "POST"
    });

    await loadData();
    const importedSnapshots = Number(res?.result?.importedSnapshots) || 0;
    const importedTrades = Number(res?.result?.importedTrades) || 0;
    const runError = res?.result?.error;

    if (runError) {
      setStatus(`抓取失败: ${runError}`, "err");
      return;
    }

    setStatus(`抓取完成：快照 ${importedSnapshots} 条，交易 ${importedTrades} 条`, "ok");
  } catch (error) {
    setStatus(`抓取失败: ${error.message}`, "err");
  }
}

async function handleRunBackfill() {
  try {
    const pagesValue = Number(els.autoConfigForm?.elements?.backfillMaxPages?.value);
    const pageSizeValue = Number(els.autoConfigForm?.elements?.backfillPageSize?.value);

    setStatus("正在回溯历史标题帖子并识别截图...", "info");
    const res = await request("/api/auto-tracking/backfill", {
      method: "POST",
      body: JSON.stringify({
        pages: Number.isFinite(pagesValue) ? pagesValue : undefined,
        pageSize: Number.isFinite(pageSizeValue) ? pageSizeValue : undefined
      })
    });

    await loadData();
    const importedSnapshots = Number(res?.result?.importedSnapshots) || 0;
    const importedTrades = Number(res?.result?.importedTrades) || 0;
    const runError = res?.result?.error;

    if (runError) {
      setStatus(`回溯失败: ${runError}`, "err");
      return;
    }

    setStatus(`回溯完成：快照 ${importedSnapshots} 条，交易 ${importedTrades} 条`, "ok");
  } catch (error) {
    setStatus(`回溯失败: ${error.message}`, "err");
  }
}

async function handleLoadCatalog() {
  try {
    setStatus("正在抓取全部月份目录...", "info");
    await fetchCatalog({ silent: true });
    setStatus(`目录抓取完成：${state.catalogPosts.length} 条月份帖子`, "ok");
  } catch (error) {
    setStatus(`目录抓取失败: ${error.message}`, "err");
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
    if (state.catalogPosts.length > 0) {
      await fetchCatalog({ silent: true });
    }

    const importedSnapshots = Number(res?.result?.importedSnapshots) || 0;
    const importedTrades = Number(res?.result?.importedTrades) || 0;
    const runError = res?.result?.error;

    if (runError) {
      setStatus(`导入失败: ${runError}`, "err");
      return;
    }

    setStatus(`导入完成：快照 ${importedSnapshots} 条，交易 ${importedTrades} 条`, "ok");
  } catch (error) {
    setStatus(`导入失败: ${error.message}`, "err");
  }
}

async function handleRecalculateSnapshots() {
  try {
    setStatus("正在重算历史快照字段并生成异常清单...", "info");
    const res = await request("/api/auto-tracking/recalculate-snapshots", {
      method: "POST"
    });
    await loadData();
    const changedSnapshots = Number(res?.summary?.changedSnapshotCount) || 0;
    const changedRows = Number(res?.summary?.changedRowCount) || 0;
    const issueRows = Number(res?.summary?.issueRowCount) || 0;
    setStatus(
      `重算完成：更新 ${changedSnapshots} 个快照，自动修正 ${changedRows} 行，仍需复核 ${issueRows} 行`,
      "ok"
    );
  } catch (error) {
    setStatus(`重算失败: ${error.message}`, "err");
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
  if (els.autoConfigForm) {
    els.autoConfigForm.addEventListener("submit", handleSaveConfig);
  }

  if (els.runAutoSyncBtn) {
    els.runAutoSyncBtn.addEventListener("click", handleRunNow);
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
  try {
    await loadData();
  } catch (error) {
    setStatus(`加载失败: ${error.message}`, "err");
  }
}

bootstrap();
