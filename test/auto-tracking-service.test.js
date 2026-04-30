const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildAutoTrackingConfigPatch,
  summarizeAutoTrackingResult,
  normalizeBackfillInput,
  normalizeSelectedImportInput,
  normalizePostIds,
  extractXueqiuPostIdFromUrl,
  classifyAutoTrackingResult
} = require("../src/auto-tracking-service");

const {
  mergeAutoTrackingConfig,
  isXueqiuTargetTitlePost,
  buildXueqiuTitleRegex
} = require("../src/super-ludinggong-sync");

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

test("normalizePostIds accepts Xueqiu post URLs and raw numeric ids", () => {
  assert.equal(extractXueqiuPostIdFromUrl("https://xueqiu.com/8790885129/386836826"), "386836826");
  assert.deepEqual(
    normalizePostIds([
      "https://xueqiu.com/8790885129/386836826",
      "386836826",
      "xq:386836826",
      "https://example.com/8790885129/386836826",
      "bad"
    ]),
    ["xq:386836826"]
  );
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

test("classifyAutoTrackingResult treats targeted zero-import failures as failed", () => {
  assert.deepEqual(
    classifyAutoTrackingResult(
      {
        ok: true,
        importedSnapshots: 0,
        importedTrades: 0,
        logs: [{ level: "error", message: "雪球帖子详情返回非 JSON（可能被风控拦截）" }]
      },
      { targeted: true, actionLabel: "指定帖子导入" }
    ),
    {
      status: "failed",
      message: "指定帖子导入未导入数据：雪球帖子详情返回非 JSON（可能被风控拦截）"
    }
  );
});

test("Xueqiu title matching accepts monthly PS titles without year", () => {
  const titleRegex = buildXueqiuTitleRegex({
    xueqiuTitleRegex: "游戏仓\\s*20\\d{2}\\s*年\\s*\\d{1,2}\\s*月\\s*PS图"
  });

  assert.equal(
    isXueqiuTargetTitlePost(
      {
        source: "xueqiu",
        title: "游戏仓4月PS图",
        text: "本游戏仓4月收盘1989.2W"
      },
      titleRegex
    ),
    true
  );
});

test("legacy Xueqiu title regex config is normalized to the relaxed default", () => {
  const config = mergeAutoTrackingConfig({
    xueqiuTitleRegex: "游戏仓\\s*20\\d{2}\\s*年\\s*\\d{1,2}\\s*月\\s*PS图"
  });

  assert.match(config.xueqiuTitleRegex, /\(\?:20\\d\{2\}/);
  assert.equal(
    isXueqiuTargetTitlePost(
      {
        source: "xueqiu",
        title: "游戏仓4月PS图",
        text: ""
      },
      buildXueqiuTitleRegex(config)
    ),
    true
  );
});
