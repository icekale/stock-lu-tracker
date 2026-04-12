# Homepage Hierarchy Hooks Implementation Plan

I'm using the writing-plans skill to create the implementation plan.

**Goal:** Establish hierarchy-aware hooks for the overview shell and section headers so downstream styling/testing can anchor on consistent selectors.

**Architecture:** Keep the existing layout untouched except for the requested selector augmentations. Add Cypress-like unit-style assertions first, then implement the minimal DOM/CSS tweaks to satisfy them before running the broader test suite.

**Tech Stack:** Node.js built-in test runner (`node:test`), static HTML/CSS, npm scripts.

---

### Task 1: Homepage hierarchy hooks

**Files:**
- Create: `test/homepage-ui-polish.test.js`
- Modify: `package.json`
- Modify: `public/index.html`
- Modify: `public/replica.css`
- Test: `test/homepage-ui-polish.test.js`

- [ ] **Step 1: Write the failing test**

```js
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
```

- [ ] **Step 2: Run the test suite and confirm the new assertions fail**

Run: `node --test test/homepage-ui-polish.test.js`
Expected: FAIL because the selectors/classes do not yet exist in `public/index.html` or `public/replica.css`.

- [ ] **Step 3: Introduce the minimal implementation**

```json
// package.json
"scripts": {
  "start": "node src/server.js",
  "dev": "node --watch src/server.js",
  "test": "node --test test/*.test.js"
}
```


```html
<div class="layui-container page-shell page-shell-home">
  <section class="layui-panel dashboard-shell">
    <div class="overview-grid" aria-label="总览统计">
      <article class="layui-card overview-card overview-card-primary">
        <div class="layui-card-body">
          ...
```

```html
<header class="section-head holdings-head">
  <div class="holdings-copy section-head-main">
    ...
  </div>
  <div class="holdings-toolbar section-head-tools">
    ...
  </div>
</header>

<header class="section-head chart-head">
  <div class="chart-head-copy section-head-main">
    ...
  </div>
</header>

<header class="section-head detail-list-head">
  <div class="detail-head-copy section-head-main">
    ...
  </div>
</header>
```

```css
.page-shell-home {
  gap: 24px;
}

.dashboard-shell-top {
  gap: 10px;
  margin-bottom: 12px;
  padding-bottom: 10px;
}

.section-head {
  gap: 12px;
  margin-bottom: 12px;
}

.section-head-main {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.section-head-tools {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  min-width: 0;
}

.overview-grid {
  border-color: rgba(15, 23, 42, 0.05);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.985), rgba(247, 250, 253, 0.95));
  box-shadow: 0 6px 18px rgba(15, 23, 42, 0.03);
}

.overview-card .layui-card-body {
  gap: 4px;
  padding: 12px 16px 11px;
}

.overview-card-primary .layui-card-body {
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.9), rgba(244, 248, 253, 0.78));
}

.overview-card-primary .metric-value {
  font-size: clamp(1.24rem, 1.32vw, 1.46rem);
}

.metric-label {
  margin-bottom: 2px;
}

.metric-meta {
  line-height: 1.3;
  opacity: 0.86;
}
```

- [ ] **Step 4: Run the entire test suite**

Run: `npm test`
Expected: PASS after the selectors and styles exist.

- [ ] **Step 5: Commit**

```bash
git add package.json public/index.html public/replica.css test/homepage-ui-polish.test.js
git commit -m "test: lock homepage hierarchy hooks"
```

Plan complete and saved to `docs/superpowers/plans/2026-04-12-homepage-hierarchy-hooks.md`. Two execution options:
1. Subagent-Driven (recommended) – launch `superpowers:subagent-driven-development` for each step with checkpoint reviews.
2. Inline Execution – run `superpowers:executing-plans` to batch steps in this session.

Which approach?
