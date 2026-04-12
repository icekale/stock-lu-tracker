const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function read(relativePath) {
  return fs.readFileSync(path.join(__dirname, "..", relativePath), "utf8");
}

test("home page exposes hierarchy hooks for the overview and section headers", () => {
  const html = read("public/index.html");

  assert.match(html, /<article class="layui-card overview-card overview-card-primary">/);
  assert.match(html, /<div class="holdings-copy section-head-main">/);
  assert.match(html, /<div class="holdings-toolbar section-head-tools">/);
  assert.match(html, /<div class="chart-head-copy section-head-main">/);
  assert.match(html, /<div class="detail-head-copy section-head-main">/);
});

test("responsive rules recompose section rhythm and controls at tablet widths", () => {
  const css = read("public/replica.css");

  assert.match(css, /@media \(max-width: 960px\)\s*\{[\s\S]*\.dashboard-shell-top\s*\{[\s\S]*gap:\s*12px;/is);
  assert.match(css, /@media \(max-width: 960px\)\s*\{[\s\S]*\.holdings-head\s*\{[\s\S]*gap:\s*10px;/is);
  assert.match(css, /@media \(max-width: 960px\)\s*\{[\s\S]*\.section-head-tools\s*\{[\s\S]*width:\s*100%;[\s\S]*justify-content:\s*flex-start;/is);
});

test("mobile rules preserve density while tightening spacing", () => {
  const css = read("public/replica.css");

  assert.match(css, /@media \(max-width: 720px\)\s*\{[\s\S]*\.page-main\s*\{[\s\S]*padding-top:\s*16px;/is);
  assert.match(css, /@media \(max-width: 720px\)\s*\{[\s\S]*\.overview-grid\s*\{[\s\S]*gap:\s*8px;/is);
  assert.match(css, /@media \(max-width: 720px\)\s*\{[\s\S]*\.dashboard-inline-meta\s*\{[\s\S]*gap:\s*6px;/is);
  assert.match(css, /@media \(max-width: 720px\)\s*\{[\s\S]*\.chart-summary-row\s*\{[\s\S]*gap:\s*10px;/is);
});

test("tablet-only `(min-width: 721px)` rules keep section heads flexy", () => {
  const css = read("public/replica.css");

  assert.match(
    css,
    /@media \(min-width: 721px\) and \(max-width: 960px\)\s*\{[\s\S]*\.section-head\s*\{[\s\S]*display:\s*flex;[\s\S]*flex-direction:\s*column;[\s\S]*gap:\s*10px;[\s\S]*margin-bottom:\s*12px;/is
  );
  assert.match(
    css,
    /@media \(min-width: 721px\) and \(max-width: 960px\)\s*\{[\s\S]*\.section-head::after\s*\{[\s\S]*display:\s*none;/is
  );
});

test("overview shell and lead metric use the refined hierarchy rules", () => {
  const css = read("public/replica.css");

  assert.match(css, /\.page-shell-home\s*\{[^}]*gap:\s*24px;/is);
  assert.match(css, /\.section-head-main\s*\{[^}]*display:\s*grid;/is);
  assert.match(css, /\.section-head-tools\s*\{[^}]*display:\s*flex;/is);
  assert.match(css, /\.overview-card-primary\s+\.metric-value\s*\{[^}]*font-size:\s*clamp\(1\.24rem,\s*1\.32vw,\s*1\.46rem\);/is);
});

test("holdings controls use quiet but clear interaction states", () => {
  const css = read("public/replica.css");

  assert.match(css, /\.holdings-view-toggle\s*\{[^}]*gap:\s*2px;[^}]*padding:\s*2px;/is);
  assert.match(css, /\.holdings-view-toggle\s+\.view-toggle-btn:not\(\.active\):hover\s*\{[^}]*background:\s*rgba\(15,\s*23,\s*42,\s*0\.04\);[^}]*color:\s*var\(--color-text-subtle-strong\);/is);
  assert.match(css, /\.holdings-view-toggle\s+\.view-toggle-btn\.active\s*\{[^}]*background:\s*rgba\(22,\s*119,\s*255,\s*0\.12\);[^}]*box-shadow:\s*none;/is);
  assert.match(css, /\.holdings-view-toggle\s+\.view-toggle-btn:focus-visible\s*\{[^}]*box-shadow:\s*var\(--shadow-focus-ring\);/is);
  assert.match(css, /\.holdings-view-toggle\s*\{[^}]*border:\s*1px\s+solid\s+rgba\(15,\s*23,\s*42,\s*0\.05\);/is);
  assert.match(css, /\.holdings-view-toggle\s+\.view-toggle-btn:hover\s*\{[^}]*transform:\s*none;/is);
});

test("secondary shells use restrained surfaces instead of stacked card depth", () => {
  const css = read("public/replica.css");

  assert.match(css, /\.holdings-shell\s+\.table-shell\s*\{[^}]*box-shadow:\s*none;/is);
  assert.match(css, /\.holdings-shell\s+\.subsection-card\s*\{[^}]*box-shadow:\s*none;/is);
  assert.match(css, /\.chart-summary-card\s*\{[^}]*box-shadow:\s*none;/is);
  assert.match(css, /\.networth-canvas-wrap\s*\{[^}]*box-shadow:\s*none;/is);
});
