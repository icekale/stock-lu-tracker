const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function read(relativePath) {
  return fs.readFileSync(path.join(__dirname, "..", relativePath), "utf8");
}

test("admin page exposes productized module navigation", () => {
  const html = read("public/admin.html");

  assert.match(html, /<nav class="admin-module-nav" aria-label="后台模块导航">/);
  assert.match(html, /data-admin-section="overview"/);
  assert.match(html, /data-admin-section="config"/);
  assert.match(html, /data-admin-section="tasks"/);
  assert.match(html, /data-admin-section="review"/);
  assert.match(html, /data-admin-section="metrics"/);
});

test("admin page wraps major areas in stable module panels", () => {
  const html = read("public/admin.html");

  assert.match(html, /<section class="admin-module-panel is-active" id="admin-section-overview" data-admin-panel="overview">/);
  assert.match(html, /<section class="admin-module-panel" id="admin-section-config" data-admin-panel="config">/);
  assert.match(html, /<section class="admin-module-panel" id="admin-section-tasks" data-admin-panel="tasks">/);
  assert.match(html, /<section class="admin-module-panel" id="admin-section-review" data-admin-panel="review">/);
  assert.match(html, /<section class="admin-module-panel" id="admin-section-metrics" data-admin-panel="metrics">/);
});

test("admin page keeps legacy element ids used by auto-sync js", () => {
  const html = read("public/admin.html");
  const ids = [
    "autoConfigForm",
    "directPostUrlInput",
    "importDirectPostBtn",
    "runCookieKeepAliveBtn",
    "runAutoSyncBtn",
    "runBackfillBtn",
    "loadCatalogBtn",
    "importSelectedBtn",
    "recalculateSnapshotsBtn",
    "systemStatusGrid",
    "syncLogsBody",
    "catalogBody",
    "anomalyRowsBody",
    "monthlyMetricBody"
  ];

  for (const id of ids) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
});

test("admin page includes job status placeholders", () => {
  const html = read("public/admin.html");

  assert.match(html, /id="jobStatusCard"/);
  assert.match(html, /id="jobStatusTitle"/);
  assert.match(html, /id="jobStatusProgress"/);
  assert.match(html, /id="jobStatusSummary"/);
});

test("auto-sync script includes admin module switching and job polling", () => {
  const js = read("public/auto-sync.js");

  assert.match(js, /function setAdminSection\(section\)/);
  assert.match(js, /document\.querySelectorAll\("\[data-admin-section\]"\)/);
  assert.match(js, /document\.querySelectorAll\("\[data-admin-panel\]"\)/);
  assert.match(js, /async function loadJobOverview\(/);
  assert.match(js, /\/api\/jobs\/overview/);
  assert.match(js, /function renderJobStatus\(overview\)/);
});

test("auto-sync script wires direct post URL import", () => {
  const js = read("public/auto-sync.js");

  assert.match(js, /directPostUrlInput:\s*document\.getElementById\("directPostUrlInput"\)/);
  assert.match(js, /importDirectPostBtn:\s*document\.getElementById\("importDirectPostBtn"\)/);
  assert.match(js, /async function handleImportDirectPost\(\)/);
  assert.match(js, /\/api\/auto-tracking\/import-post-url/);
  assert.match(js, /resolveAutoTrackingResultStatus\(res\?\.result,\s*"导入",\s*res\?\.job\)/);
  assert.match(js, /els\.importDirectPostBtn\.addEventListener\("click",\s*handleImportDirectPost\)/);
});

test("admin css defines module shell, job status, and overflow safeguards", () => {
  const css = read("public/admin.css");

  assert.match(css, /\.admin-module-nav\s*\{/);
  assert.match(css, /\.admin-module-tab\.is-active\s*\{/);
  assert.match(css, /\.admin-module-panel\s*\{/);
  assert.match(css, /\.admin-module-panel\.is-active\s*\{/);
  assert.match(css, /\.job-status-card\s*\{/);
  assert.match(css, /\.job-status-progress\s*\{/);
  assert.match(css, /\.table-shell\s*\{[\s\S]*overflow-x:\s*auto;/);
  assert.match(css, /overflow-wrap:\s*anywhere;/);
});

test("admin css keeps the product shell compact and stable on narrow screens", () => {
  const css = read("public/admin.css");

  assert.match(css, /\.page-shell-admin\s*\{[^}]*width:\s*min\(var\(--shell-width\),\s*100%\);[^}]*padding:\s*0;/is);
  assert.match(css, /\.hero-panel\s*\{[^}]*grid-template-columns:\s*minmax\(0,\s*1fr\)\s*auto;/is);
  assert.match(css, /\.system-status-card\s*\{[^}]*box-shadow:\s*none;/is);
  assert.match(css, /@media \(max-width:\s*640px\)\s*\{[\s\S]*\.hero-subtitle\s*\{[\s\S]*-webkit-line-clamp:\s*2;/is);
  assert.match(css, /@media \(max-width:\s*720px\)\s*\{[\s\S]*\.admin-module-nav\s*\{[\s\S]*scroll-snap-type:\s*x\s+proximity;/is);
});
