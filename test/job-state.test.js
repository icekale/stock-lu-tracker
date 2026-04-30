const test = require("node:test");
const assert = require("node:assert/strict");

const {
  createJob,
  startJob,
  updateJob,
  finishJob,
  failJob,
  skipJob,
  getJob,
  getJobOverview,
  resetJobsForTests
} = require("../src/job-state");

test.beforeEach(() => {
  resetJobsForTests();
});

test("job lifecycle records running and succeeded states", () => {
  const job = createJob("auto_tracking_run", { label: "立即抓取" });

  assert.equal(job.status, "queued");
  assert.equal(job.type, "auto_tracking_run");
  assert.match(job.jobId, /^job_/);

  startJob(job.jobId, { stage: "collect", message: "开始抓取" });
  updateJob(job.jobId, { stage: "import", progress: 60, message: "导入快照" });
  const finished = finishJob(job.jobId, {
    summary: { importedSnapshots: 2, importedTrades: 3 },
    message: "完成"
  });

  assert.equal(finished.status, "succeeded");
  assert.equal(finished.stage, "completed");
  assert.equal(finished.progress, 100);
  assert.equal(finished.summary.importedSnapshots, 2);
  assert.ok(finished.finishedAt);

  const overview = getJobOverview();
  assert.equal(overview.running, null);
  assert.equal(overview.recent.length, 1);
  assert.equal(overview.recent[0].jobId, job.jobId);
});

test("job failure serializes safe error details", () => {
  const job = createJob("auto_tracking_backfill", { label: "历史回溯" });
  startJob(job.jobId, { stage: "collect" });

  const failed = failJob(job.jobId, new Error("Cookie expired: xq_a_token=secret"), {
    stage: "collect",
    message: "抓取失败"
  });

  assert.equal(failed.status, "failed");
  assert.equal(failed.stage, "collect");
  assert.equal(failed.error.message, "Cookie expired: [redacted]");
  assert.equal(failed.message, "抓取失败");
});

test("skipJob records a skipped succeeded result", () => {
  const job = createJob("cookie_keepalive", { label: "Cookie 保活" });
  const skipped = skipJob(job.jobId, "Cookie 保活已关闭");

  assert.equal(skipped.status, "succeeded");
  assert.equal(skipped.skipped, true);
  assert.equal(skipped.summary.reason, "Cookie 保活已关闭");
});

test("recent history is bounded", () => {
  for (let index = 0; index < 42; index += 1) {
    const job = createJob("auto_tracking_run", { label: `任务 ${index}` });
    finishJob(job.jobId, { summary: { index } });
  }

  const overview = getJobOverview();
  assert.equal(overview.recent.length, 30);
  assert.equal(overview.recent[0].summary.index, 41);
  assert.equal(overview.recent.at(-1).summary.index, 12);
});

test("getJob returns null for unknown ids", () => {
  assert.equal(getJob("missing"), null);
});

test("stale running jobs are expired from the running slot", () => {
  const job = createJob("auto_tracking_run", { label: "立即抓取" });
  startJob(job.jobId, { stage: "collect", message: "开始抓取" });

  const overview = getJobOverview({ now: "2099-01-01T00:00:00.000Z" });

  assert.equal(overview.running, null);
  assert.equal(overview.recent.length, 1);
  assert.equal(overview.recent[0].jobId, job.jobId);
  assert.equal(overview.recent[0].status, "failed");
  assert.equal(overview.recent[0].stage, "stale");
  assert.match(overview.recent[0].error.message, /任务运行时间过长/);
});
