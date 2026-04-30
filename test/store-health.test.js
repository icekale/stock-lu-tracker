const test = require("node:test");
const assert = require("node:assert/strict");

const { getStorePaths, getStoreHealthSummary } = require("../src/store");

test("getStorePaths exposes expected local data paths", () => {
  const paths = getStorePaths();

  assert.match(paths.dataDir, /data$/);
  assert.match(paths.storePath, /data\/store\.json$/);
  assert.match(paths.backupPath, /data\/store\.json\.bak$/);
});

test("getStoreHealthSummary returns safe metadata", async () => {
  const summary = await getStoreHealthSummary();

  assert.equal(typeof summary.exists, "boolean");
  assert.equal(typeof summary.backupExists, "boolean");
  assert.equal(typeof summary.readable, "boolean");
  assert.equal(typeof summary.storePath, "string");
  assert.equal(summary.storePath.includes("store.json"), true);
  assert.equal(JSON.stringify(summary).includes("xq_a_token"), false);
  assert.equal(JSON.stringify(summary).includes("qwenApiKey"), false);
});
