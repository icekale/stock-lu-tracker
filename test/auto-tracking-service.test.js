const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildAutoTrackingConfigPatch,
  summarizeAutoTrackingResult,
  normalizeBackfillInput,
  normalizeSelectedImportInput
} = require("../src/auto-tracking-service");

test("buildAutoTrackingConfigPatch preserves secrets when fields are omitted or empty", () => {
  const current = {
    enabled: true,
    xueqiuCookie: "saved-xq",
    weiboCookie: "saved-wb",
    qwenApiKey: "saved-key",
    keywords: ["持仓"]
  };

  const patch = buildAutoTrackingConfigPatch(current, {
    enabled: false,
    xueqiuCookie: "",
    keywords: "调仓\n持仓"
  });

  assert.equal(patch.enabled, false);
  assert.equal(patch.xueqiuCookie, "saved-xq");
  assert.equal(patch.weiboCookie, "saved-wb");
  assert.equal(patch.qwenApiKey, "saved-key");
  assert.deepEqual(patch.keywords, ["调仓", "持仓"]);
});

test("buildAutoTrackingConfigPatch supports explicit secret clearing", () => {
  const patch = buildAutoTrackingConfigPatch(
    { xueqiuCookie: "saved-xq", weiboCookie: "saved-wb", qwenApiKey: "saved-key" },
    { clearXueqiuCookie: true, clearQwenApiKey: true }
  );

  assert.equal(patch.xueqiuCookie, "");
  assert.equal(patch.weiboCookie, "saved-wb");
  assert.equal(patch.qwenApiKey, "");
});

test("normalizeBackfillInput clamps optional numeric inputs", () => {
  assert.deepEqual(normalizeBackfillInput({ pages: "12", pageSize: "30" }), { pages: 12, pageSize: 30 });
  assert.deepEqual(normalizeBackfillInput({ pages: "", pageSize: null }), { pages: undefined, pageSize: undefined });
});

test("normalizeSelectedImportInput validates selected post ids", () => {
  assert.deepEqual(normalizeSelectedImportInput({ postIds: ["xq:123456", "bad", "wb:ABCDEF1"] }), {
    postIds: ["xq:123456", "wb:ABCDEF1"],
    pages: undefined,
    pageSize: undefined
  });
});

test("summarizeAutoTrackingResult produces compact safe summaries", () => {
  const summary = summarizeAutoTrackingResult({
    ok: true,
    mode: "backfill",
    importedSnapshots: 2,
    importedTrades: 5,
    skippedSnapshots: 1,
    logs: [{ message: "a" }, { message: "b" }]
  });

  assert.deepEqual(summary, {
    ok: true,
    mode: "backfill",
    importedSnapshots: 2,
    importedTrades: 5,
    skippedSnapshots: 1,
    logCount: 2,
    error: ""
  });
});
