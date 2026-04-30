const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function read(relativePath) {
  return fs.readFileSync(path.join(__dirname, "..", relativePath), "utf8");
}

test("server exposes job overview and detail endpoints", () => {
  const server = read("src/server.js");

  assert.match(server, /const \{[\s\S]*createJob[\s\S]*getJobOverview[\s\S]*\} = require\("\.\/job-state"\);/);
  assert.match(server, /app\.use\("\/api\/jobs", requireAdminAuth\);/);
  assert.match(server, /app\.get\("\/api\/jobs\/overview",\s*\(_req,\s*res\)/);
  assert.match(server, /app\.get\("\/api\/jobs\/:jobId",\s*\(req,\s*res/);
});

test("auto tracking actions create and finish observable jobs", () => {
  const server = read("src/server.js");

  assert.match(server, /const job = createJob\("auto_tracking_run"/);
  assert.match(server, /startJob\(job\.jobId,\s*\{[\s\S]*stage: "collect"/);
  assert.match(server, /finishJob\(job\.jobId, \{[\s\S]*summarizeAutoTrackingResult\(result\)/);
  assert.match(server, /failJob\(job\.jobId, error/);
  assert.match(server, /const job = createJob\("cookie_keepalive"/);
});

test("server exposes direct post URL import as an observable job", () => {
  const server = read("src/server.js");

  assert.match(server, /app\.post\("\/api\/auto-tracking\/import-post-url"/);
  assert.match(server, /createJob\("auto_tracking_import_post_url"/);
  assert.match(server, /targetPostIds:\s*postIds/);
  assert.match(server, /forceRefresh:\s*true/);
});

test("scheduled and startup auto tracking are surfaced as observable jobs", () => {
  const server = read("src/server.js");

  assert.match(server, /async function runAutoTrackingWithJob\(trigger = "manual", collectOptions = \{\}, jobOptions = \{\}\)/);
  assert.match(server, /runAutoTrackingWithJob\("timer"/);
  assert.match(server, /runAutoTrackingWithJob\("startup"/);
  assert.match(server, /const \{ result, job \} = await runAutoTrackingWithJob\("manual"/);
});
