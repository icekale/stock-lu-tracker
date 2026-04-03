const fs = require("node:fs/promises");
const path = require("node:path");

const DATA_DIR = path.join(process.cwd(), "data");
const STORE_PATH = path.join(DATA_DIR, "store.json");

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

async function ensureStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  try {
    await fs.access(STORE_PATH);
  } catch {
    await writeStore(DEFAULT_STORE);
  }
}

async function readStore() {
  await ensureStore();
  const raw = await fs.readFile(STORE_PATH, "utf8");

  try {
    const parsed = JSON.parse(raw);
    return {
      ...DEFAULT_STORE,
      ...parsed,
      autoTracking: {
        ...DEFAULT_STORE.autoTracking,
        ...(parsed.autoTracking || {}),
        config: {
          ...DEFAULT_STORE.autoTracking.config,
          ...(parsed.autoTracking?.config || {})
        },
        runtime: {
          ...DEFAULT_STORE.autoTracking.runtime,
          ...(parsed.autoTracking?.runtime || {})
        },
        processedPostIds: Array.isArray(parsed.autoTracking?.processedPostIds)
          ? parsed.autoTracking.processedPostIds
          : [],
        importedTradeKeys: Array.isArray(parsed.autoTracking?.importedTradeKeys)
          ? parsed.autoTracking.importedTradeKeys
          : [],
        logs: Array.isArray(parsed.autoTracking?.logs) ? parsed.autoTracking.logs : []
      },
      masterSnapshots: Array.isArray(parsed.masterSnapshots) ? parsed.masterSnapshots : [],
      settings: {
        ...DEFAULT_STORE.settings,
        ...(parsed.settings || {})
      }
    };
  } catch {
    await writeStore(DEFAULT_STORE);
    return structuredClone(DEFAULT_STORE);
  }
}

async function writeStore(store) {
  await fs.mkdir(DATA_DIR, { recursive: true });
  const tmpPath = `${STORE_PATH}.tmp`;
  await fs.writeFile(tmpPath, JSON.stringify(store, null, 2), "utf8");
  await fs.rename(tmpPath, STORE_PATH);
}

async function mutateStore(mutator) {
  let result;
  mutationQueue = mutationQueue.then(async () => {
    const store = await readStore();
    result = await mutator(store);
    await writeStore(store);
  });

  await mutationQueue;
  return result;
}

module.exports = {
  readStore,
  mutateStore,
  ensureStore
};
