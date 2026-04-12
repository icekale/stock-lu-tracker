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

test("overview shell and lead metric use the refined hierarchy rules", () => {
  const css = read("public/replica.css");

  assert.match(css, /\.page-shell-home\s*\{[^}]*gap:\s*24px;/is);
  assert.match(css, /\.section-head-main\s*\{[^}]*display:\s*grid;/is);
  assert.match(css, /\.section-head-tools\s*\{[^}]*display:\s*flex;/is);
  assert.match(css, /\.overview-card-primary\s+\.metric-value\s*\{[^}]*font-size:\s*clamp\(1\.24rem,\s*1\.32vw,\s*1\.46rem\);/is);
});
