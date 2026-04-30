const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function read(relativePath) {
  return fs.readFileSync(path.join(__dirname, "..", relativePath), "utf8");
}

test("financial design tokens follow A-share convention", () => {
  const css = read("public/design-tokens.css");

  assert.match(css, /--color-data-positive:\s*#b05b63;/i);
  assert.match(css, /--color-data-negative:\s*#23714a;/i);
  assert.match(css, /--gain:\s*var\(--color-danger\);/i);
  assert.match(css, /--loss:\s*var\(--color-success\);/i);
});

test("replica holdings badges use red for buy and green for sell", () => {
  const css = read("public/replica.css");

  assert.match(css, /\.badge\.buy\s*\{[^}]*color:\s*var\(--color-danger-strong\);/is);
  assert.match(css, /\.badge\.sell\s*\{[^}]*color:\s*var\(--color-success-strong\);/is);
});

test("admin script separates operational status classes from financial pnl classes", () => {
  const js = read("public/auto-sync.js");

  assert.match(js, /classList\.remove\("status-ok",\s*"status-err"\)/);
  assert.match(js, /classList\.add\("status-ok"\)/);
  assert.match(js, /classList\.add\("status-err"\)/);
  assert.match(js, /const pnlClass = floatingPnlRaw > 0 \? "finance-pos" : floatingPnlRaw < 0 \? "finance-neg" : "";/);
  assert.match(js, /const pctClass = pnlPctRaw > 0 \? "finance-pos" : pnlPctRaw < 0 \? "finance-neg" : "";/);
});

test("admin styles keep success or error messaging separate from financial colors", () => {
  const css = read("public/admin.css");

  assert.match(css, /\.status-ok\s*\{[^}]*color:\s*var\(--color-success-strong\);/is);
  assert.match(css, /\.status-err\s*\{[^}]*color:\s*var\(--color-danger-strong\);/is);
  assert.match(css, /\.finance-pos\s*\{[^}]*color:\s*var\(--color-danger-strong\);/is);
  assert.match(css, /\.finance-neg\s*\{[^}]*color:\s*var\(--color-success-strong\);/is);
});
