const fs = require("node:fs/promises");
const path = require("node:path");

const DATA_DIR = path.join(process.cwd(), "data");
const STORE_PATH = path.join(DATA_DIR, "store.json");
const STORE_BACKUP_PATH = `${STORE_PATH}.bak`;
const STORE_FLUSH_DELAY_MS = Math.max(25, Number(process.env.STORE_FLUSH_DELAY_MS) || 80);

const DEFAULT_STORE = {
  trades: [],
  quotes: {},
  snapshots: [],
  monthlyUpdates: [],
  masterSnapshots: [],
  autoTracking: {
    config: {
      enabled: true,
      intervalMinutes: 180,
      smartScheduleEnabled: true,
      monthEndWindowDays: 2,
      offWindowIntervalHours: 72,
      skipStartupOutsideWindow: true,
      cookieKeepAliveEnabled: true,
      cookieKeepAliveIntervalHours: 12,
      xueqiuCookie: "",
      weiboCookie: "",
      maxPostsPerSource: 6,
      ocrEnabled: true,
      ocrProvider: "auto",
      ocrMaxImagesPerPost: 2,
      qwenApiKey: "",
      pinnedPostUrls: ["https://xueqiu.com/8790885129/381996320"],
      xueqiuTitleRegex: "游戏仓\\s*20\\d{2}\\s*年\\s*\\d{1,2}\\s*月\\s*PS图",
      backfillMaxPages: 36,
      backfillPageSize: 20,
      keywords: ["最新持仓", "调仓", "新开仓", "已清仓", "持仓", "组合"]
    },
    runtime: {
      lastRunAt: null,
      lastSuccessAt: null,
      lastError: null,
      nextRunAt: null,
      lastCookieKeepAliveAt: null,
      lastCookieKeepAliveSuccessAt: null,
      lastCookieKeepAliveError: null,
      nextCookieKeepAliveAt: null,
      scheduleMode: null,
      scheduleHint: "",
      totalImportedSnapshots: 0,
      totalImportedTrades: 0
    },
    processedPostIds: [],
    importedTradeKeys: [],
    logs: [],
    latestSnapshot: null
  },
  settings: {
    baseCurrency: "CNY"
  }
};

let mutationQueue = Promise.resolve();
let storeCache = null;
let storeLoadPromise = null;
let flushTimer = null;
let flushPromise = Promise.resolve();
let dirtySinceFlush = false;
let flushHooksRegistered = false;
let shutdownFlushRunning = false;

function normalizeStore(parsed) {
  return {
    ...DEFAULT_STORE,
    ...parsed,
    trades: Array.isArray(parsed?.trades) ? parsed.trades : [],
    quotes: parsed?.quotes && typeof parsed.quotes === "object" ? parsed.quotes : {},
    snapshots: Array.isArray(parsed?.snapshots) ? parsed.snapshots : [],
    monthlyUpdates: Array.isArray(parsed?.monthlyUpdates) ? parsed.monthlyUpdates : [],
    autoTracking: {
      ...DEFAULT_STORE.autoTracking,
      ...(parsed?.autoTracking || {}),
      config: {
        ...DEFAULT_STORE.autoTracking.config,
        ...(parsed?.autoTracking?.config || {})
      },
      runtime: {
        ...DEFAULT_STORE.autoTracking.runtime,
        ...(parsed?.autoTracking?.runtime || {})
      },
      processedPostIds: Array.isArray(parsed?.autoTracking?.processedPostIds)
        ? parsed.autoTracking.processedPostIds
        : [],
      importedTradeKeys: Array.isArray(parsed?.autoTracking?.importedTradeKeys)
        ? parsed.autoTracking.importedTradeKeys
        : [],
      logs: Array.isArray(parsed?.autoTracking?.logs) ? parsed.autoTracking.logs : []
    },
    masterSnapshots: Array.isArray(parsed?.masterSnapshots) ? parsed.masterSnapshots : [],
    settings: {
      ...DEFAULT_STORE.settings,
      ...(parsed?.settings || {})
    }
  };
}

async function writeStore(store) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  const tmpPath = `${STORE_PATH}.${process.pid}.${Date.now()}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(store, null, 2), "utf8");
  await fs.rename(tmpPath, STORE_PATH);

  try {
    await fs.copyFile(STORE_PATH, STORE_BACKUP_PATH);
  } catch (error) {
    console.error("Failed to update store backup:", error.message);
  }
}

async function readStoreFile(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return normalizeStore(JSON.parse(raw));
}

async function readBackupStore() {
  try {
    return await readStoreFile(STORE_BACKUP_PATH);
  } catch {
    return null;
  }
}

async function preserveCorruptedStore() {
  const corruptPath = path.join(DATA_DIR, `store.corrupt-${Date.now()}.json`);
  await fs.copyFile(STORE_PATH, corruptPath);
  return corruptPath;
}

async function loadStoreFromDisk() {
  await fs.mkdir(DATA_DIR, { recursive: true });

  try {
    await fs.access(STORE_PATH);
  } catch {
    const backupStore = await readBackupStore();
    if (backupStore) {
      console.warn("Primary store is missing, restored from backup");
      await writeStore(backupStore);
      return backupStore;
    }

    const fallback = structuredClone(DEFAULT_STORE);
    await writeStore(fallback);
    return fallback;
  }

  try {
    return await readStoreFile(STORE_PATH);
  } catch (error) {
    const preservedPath = await preserveCorruptedStore().catch(() => null);
    const backupStore = await readBackupStore();

    if (backupStore) {
      console.warn(
        `Store file is corrupted, restored from backup${preservedPath ? ` and preserved at ${preservedPath}` : ""}`
      );
      await writeStore(backupStore);
      return backupStore;
    }

    console.error("Failed to parse store file:", error.message);
    if (preservedPath) {
      console.warn(`Corrupted store file preserved at ${preservedPath}`);
    }
    const fallback = structuredClone(DEFAULT_STORE);
    await writeStore(fallback);
    return fallback;
  }
}

async function ensureStore() {
  if (storeCache) {
    return;
  }

  if (!storeLoadPromise) {
    storeLoadPromise = loadStoreFromDisk()
      .then((store) => {
        storeCache = store;
      })
      .finally(() => {
        storeLoadPromise = null;
      });
  }

  await storeLoadPromise;
}

function scheduleFlush() {
  dirtySinceFlush = true;
  if (flushTimer) {
    return;
  }

  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushStore().catch((error) => {
      console.error("Failed to flush store cache:", error.message);
    });
  }, STORE_FLUSH_DELAY_MS);
}

async function flushStore() {
  await ensureStore();

  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }

  if (!dirtySinceFlush) {
    await flushPromise;
    return;
  }

  const snapshot = structuredClone(storeCache);
  dirtySinceFlush = false;

  flushPromise = flushPromise.catch(() => {}).then(async () => {
    await writeStore(snapshot);
  });

  try {
    await flushPromise;
  } catch (error) {
    dirtySinceFlush = true;
    throw error;
  } finally {
    if (dirtySinceFlush && !flushTimer) {
      scheduleFlush();
    }
  }
}

function registerFlushHooks() {
  if (flushHooksRegistered) {
    return;
  }
  flushHooksRegistered = true;

  process.once("beforeExit", async () => {
    if (!dirtySinceFlush) {
      return;
    }
    try {
      await flushStore();
    } catch (error) {
      console.error("Failed to flush store before exit:", error.message);
    }
  });

  const flushAndExit = (signal) => {
    if (shutdownFlushRunning) {
      return;
    }
    shutdownFlushRunning = true;

    flushStore()
      .catch((error) => {
        console.error(`Failed to flush store on ${signal}:`, error.message);
      })
      .finally(() => {
        process.exit(0);
      });
  };

  process.once("SIGINT", () => flushAndExit("SIGINT"));
  process.once("SIGTERM", () => flushAndExit("SIGTERM"));
}

async function readStore() {
  await ensureStore();
  return structuredClone(storeCache);
}

async function mutateStore(mutator) {
  let result;

  const run = mutationQueue.then(async () => {
    await ensureStore();
    const draft = structuredClone(storeCache);
    result = await mutator(draft);
    storeCache = draft;
    scheduleFlush();
  });

  mutationQueue = run.catch(() => {});
  await run;
  return result;
}

module.exports = {
  readStore,
  mutateStore,
  ensureStore,
  flushStore
};

registerFlushHooks();
