const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function read(relativePath) {
  return fs.readFileSync(path.join(__dirname, "..", relativePath), "utf8");
}

function escapeRegex(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

test("footer version fallbacks stay aligned with package version", () => {
  const packageJson = JSON.parse(read("package.json"));
  const versionLabel = `v${packageJson.version}`;
  const versionPattern = new RegExp(`data-fallback="${escapeRegex(versionLabel)}"[\\s\\S]*?>${escapeRegex(versionLabel)}<`, "u");

  assert.match(read("public/index.html"), versionPattern);
  assert.match(read("public/admin.html"), versionPattern);
  assert.match(read("public/admin-login.html"), versionPattern);
  assert.match(read("public/site-footer.js"), new RegExp(`versionLabel:\\s*versionEl\\?\\.getAttribute\\("data-fallback"\\)\\s*\\|\\|\\s*"${escapeRegex(versionLabel)}"`));
});

test("server disables caching for footer version metadata surfaces", () => {
  const server = read("src/server.js");

  assert.match(server, /function setNoStoreHeaders\(res\)\s*\{[\s\S]*Cache-Control[\s\S]*no-store[\s\S]*\}/);
  assert.match(server, /function setPublicAssetCacheHeaders\(res,\s*filePath\)\s*\{[\s\S]*\.html[\s\S]*site-footer\.js[\s\S]*setNoStoreHeaders\(res\);[\s\S]*\}/);
  assert.match(server, /app\.get\("\/api\/app-meta",\s*\(_req,\s*res\)\s*=>\s*\{[\s\S]*setNoStoreHeaders\(res\);[\s\S]*res\.json\(APP_META\);[\s\S]*\}\);/);
  assert.match(server, /app\.get\("\/admin\.html",\s*requireAdminAuth,\s*\(_req,\s*res\)\s*=>\s*\{[\s\S]*setNoStoreHeaders\(res\);[\s\S]*res\.sendFile\(/);
  assert.match(
    server,
    /app\.use\(\s*express\.static\(path\.join\(process\.cwd\(\),\s*"public"\),\s*\{[\s\S]*setHeaders:\s*setPublicAssetCacheHeaders[\s\S]*\}\)\s*\);/
  );
});

test("health endpoint includes store and job summaries", () => {
  const server = read("src/server.js");

  assert.match(server, /const \{[\s\S]*getStoreHealthSummary[\s\S]*\} = require\("\.\/store"\);/);
  assert.match(server, /app\.get\("\/api\/health",\s*async \(_req,\s*res,\s*next\)\s*=>/);
  assert.match(server, /store:\s*await getStoreHealthSummary\(\)/);
  assert.match(server, /jobs:\s*getJobOverview\(\)/);
});
