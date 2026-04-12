const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function createElementStub() {
  return {
    textContent: "",
    innerHTML: "",
    hidden: false,
    dataset: {},
    style: {},
    disabled: false,
    value: "",
    title: "",
    classList: {
      add() {},
      remove() {},
      toggle() {}
    },
    addEventListener() {},
    setAttribute() {},
    removeAttribute() {},
    getAttribute() {
      return null;
    },
    closest() {
      return null;
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    appendChild() {},
    replaceChildren() {},
    focus() {},
    scrollTo() {},
    getContext() {
      return {};
    },
    scrollLeft: 0,
    scrollWidth: 0,
    clientWidth: 0
  };
}

function loadHomeAppModule() {
  const sourcePath = path.join(__dirname, "..", "public", "app.js");
  const source =
    fs.readFileSync(sourcePath, "utf8").replace(/\nbootstrap\(\);\s*$/u, "\n") +
    "\nmodule.exports = { state, els, buildRenderPayload, buildMonthlySeries, renderOverviewStats };";

  const elements = new Map();
  const context = {
    module: { exports: {} },
    exports: {},
    console,
    window: {
      matchMedia: () => ({ matches: false }),
      requestAnimationFrame: (callback) => {
        if (typeof callback === "function") {
          callback();
        }
        return 1;
      }
    },
    document: {
      getElementById(id) {
        if (!elements.has(id)) {
          elements.set(id, createElementStub());
        }
        return elements.get(id);
      },
      addEventListener() {},
      activeElement: null
    },
    HTMLElement: function HTMLElement() {},
    Chart: function Chart() {},
    fetch: async () => ({
      ok: true,
      async json() {
        return { snapshots: [] };
      }
    })
  };

  vm.createContext(context);
  vm.runInContext(source, context, { filename: sourcePath });
  return context.module.exports;
}

function createSnapshot(overrides = {}) {
  return {
    id: "snapshot-1",
    postedAt: "2026-03-31T07:10:44.000Z",
    title: "2026年3月 持仓快照",
    source: "xueqiu",
    rawText: "",
    ocrText: "",
    cumulativeNetValue: 19_339_000,
    netIndex: 10.9972,
    yearStartNetIndex: 10.023,
    rows: [
      {
        symbol: "600900",
        name: "长江电力",
        holdingQty: 185000,
        latestCost: 26.63,
        referenceCost: 26.63,
        latestPrice: 27.04,
        marketValue: 5_002_400
      },
      {
        symbol: "511880",
        name: "银华日利",
        holdingQty: 25000,
        latestCost: 99.84,
        referenceCost: 99.84,
        latestPrice: 100.312,
        marketValue: 2_507_800
      }
    ],
    ...overrides
  };
}

test("overview total market value comes from holdings value, not cumulative net value", () => {
  const app = loadHomeAppModule();
  const snapshot = createSnapshot();

  app.state.snapshots = [snapshot];
  app.state.selectedSnapshotId = snapshot.id;

  const payload = app.buildRenderPayload();

  assert.equal(payload.currentMarketValue, 7_510_200);
  assert.equal(payload.currentPostMetrics.cumulativeNetValue, 19_339_000);
});

test("overview cumulative net value card does not fall back to total market value", () => {
  const app = loadHomeAppModule();

  app.renderOverviewStats({
    current: null,
    currentMarketValue: 7_510_200,
    currentPostMetrics: {
      cumulativeNetValue: null,
      netIndex: null,
      yearStartNetIndex: null
    },
    holdingCount: 2,
    previousMarketValue: null
  });

  assert.equal(app.els.totalMarketValue.textContent, "¥ 7,510,200");
  assert.equal(app.els.cumulativeNetValue.textContent, "-");
});

test("monthly chart series still prefers cumulative net value when it exists", () => {
  const app = loadHomeAppModule();
  const older = createSnapshot({
    id: "snapshot-older",
    postedAt: "2026-02-28T07:10:44.000Z",
    title: "2026年2月 持仓快照",
    cumulativeNetValue: 18_885_000
  });
  const current = createSnapshot();

  app.state.snapshots = [current, older];
  app.state.selectedSnapshotId = current.id;

  const series = app.buildMonthlySeries();

  assert.deepEqual(
    JSON.parse(JSON.stringify(series.map((item) => ({ month: item.month, value: item.value })))),
    [
      { month: "2026-02", value: 18_885_000 },
      { month: "2026-03", value: 19_339_000 }
    ]
  );
});
