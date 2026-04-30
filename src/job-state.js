const { randomUUID } = require("node:crypto");

const MAX_RECENT_JOBS = 30;
const jobs = new Map();
let recentJobIds = [];

function nowIso() {
  return new Date().toISOString();
}

function redactSensitiveText(value) {
  return String(value || "")
    .replace(/(?:xq_a_token|xqat|xq_r_token|SUB|qwenApiKey|api[_-]?key)=[^;\s]+/gi, "[redacted]")
    .replace(/sk-[A-Za-z0-9_-]{12,}/g, "[redacted]");
}

function cloneJob(job) {
  return job ? structuredClone(job) : null;
}

function rememberRecent(jobId) {
  recentJobIds = [jobId, ...recentJobIds.filter((id) => id !== jobId)].slice(0, MAX_RECENT_JOBS);
}

function createJob(type, options = {}) {
  const createdAt = nowIso();
  const job = {
    jobId: `job_${createdAt.replace(/[-:.TZ]/g, "")}_${randomUUID().slice(0, 8)}`,
    type: String(type || "unknown"),
    label: String(options.label || type || "任务"),
    status: "queued",
    stage: "queued",
    progress: 0,
    message: String(options.message || "等待执行"),
    skipped: false,
    createdAt,
    startedAt: null,
    finishedAt: null,
    summary: {},
    logs: [],
    error: null
  };
  jobs.set(job.jobId, job);
  return cloneJob(job);
}

function requireJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) {
    throw new Error(`job not found: ${jobId}`);
  }
  return job;
}

function startJob(jobId, patch = {}) {
  const job = requireJob(jobId);
  job.status = "running";
  job.stage = String(patch.stage || "running");
  job.progress = Number.isFinite(Number(patch.progress)) ? Number(patch.progress) : 5;
  job.message = String(patch.message || "任务执行中");
  job.startedAt = job.startedAt || nowIso();
  return cloneJob(job);
}

function updateJob(jobId, patch = {}) {
  const job = requireJob(jobId);
  if (typeof patch.stage !== "undefined") {
    job.stage = String(patch.stage || job.stage);
  }
  if (typeof patch.progress !== "undefined") {
    job.progress = Math.max(0, Math.min(99, Number(patch.progress) || 0));
  }
  if (typeof patch.message !== "undefined") {
    job.message = String(patch.message || "");
  }
  if (Array.isArray(patch.logs)) {
    job.logs = patch.logs.slice(-80);
  }
  if (patch.summary && typeof patch.summary === "object") {
    job.summary = { ...job.summary, ...patch.summary };
  }
  return cloneJob(job);
}

function finishJob(jobId, patch = {}) {
  const job = requireJob(jobId);
  job.status = "succeeded";
  job.stage = "completed";
  job.progress = 100;
  job.message = String(patch.message || "任务完成");
  job.finishedAt = nowIso();
  job.summary = patch.summary && typeof patch.summary === "object" ? structuredClone(patch.summary) : job.summary;
  if (Array.isArray(patch.logs)) {
    job.logs = patch.logs.slice(-80);
  }
  rememberRecent(job.jobId);
  return cloneJob(job);
}

function failJob(jobId, error, patch = {}) {
  const job = requireJob(jobId);
  const message = redactSensitiveText(error?.message || error || "任务失败");
  job.status = "failed";
  job.stage = String(patch.stage || job.stage || "failed");
  job.progress = Math.max(0, Math.min(100, Number(job.progress) || 0));
  job.message = String(patch.message || "任务失败");
  job.finishedAt = nowIso();
  job.error = { message };
  if (patch.summary && typeof patch.summary === "object") {
    job.summary = { ...job.summary, ...patch.summary };
  }
  rememberRecent(job.jobId);
  return cloneJob(job);
}

function skipJob(jobId, reason) {
  const job = requireJob(jobId);
  job.status = "succeeded";
  job.stage = "skipped";
  job.progress = 100;
  job.message = String(reason || "任务已跳过");
  job.skipped = true;
  job.finishedAt = nowIso();
  job.summary = { reason: String(reason || "任务已跳过") };
  rememberRecent(job.jobId);
  return cloneJob(job);
}

function getJob(jobId) {
  return cloneJob(jobs.get(jobId));
}

function getJobOverview() {
  const all = [...jobs.values()];
  const running = all.find((job) => job.status === "running") || null;
  const queued = all.filter((job) => job.status === "queued").map(cloneJob);
  const recent = recentJobIds.map((id) => jobs.get(id)).filter(Boolean).map(cloneJob);
  return {
    running: cloneJob(running),
    queued,
    recent
  };
}

function resetJobsForTests() {
  jobs.clear();
  recentJobIds = [];
}

module.exports = {
  createJob,
  startJob,
  updateJob,
  finishJob,
  failJob,
  skipJob,
  getJob,
  getJobOverview,
  resetJobsForTests,
  redactSensitiveText
};
