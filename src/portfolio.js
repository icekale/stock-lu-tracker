const { normalizeSecurityName } = require("./symbols");

function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function toDateSafe(value) {
  const date = value ? new Date(value) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return null;
  }
  return date;
}

function sortTradesAsc(a, b) {
  const aDate = toDateSafe(a.tradeDate)?.getTime() ?? 0;
  const bDate = toDateSafe(b.tradeDate)?.getTime() ?? 0;

  if (aDate !== bDate) {
    return aDate - bDate;
  }

  const aCreated = toDateSafe(a.createdAt)?.getTime() ?? 0;
  const bCreated = toDateSafe(b.createdAt)?.getTime() ?? 0;

  if (aCreated !== bCreated) {
    return aCreated - bCreated;
  }

  return String(a.id || "").localeCompare(String(b.id || ""));
}

function buildPortfolio(trades, quotes) {
  const symbolStates = new Map();

  for (const trade of [...trades].sort(sortTradesAsc)) {
    const apiSymbol = trade.apiSymbol || trade.symbol;
    if (!apiSymbol) {
      continue;
    }

    if (!symbolStates.has(apiSymbol)) {
        symbolStates.set(apiSymbol, {
          apiSymbol,
          rawSymbol: trade.symbol,
          market: trade.market,
          nameSource: String(trade.nameSource || "").trim().toLowerCase(),
          name: normalizeSecurityName(trade.apiSymbol || trade.symbol, trade.name, trade.market, {
            nameSource: trade.nameSource
          }),
          quantity: 0,
          avgCost: 0,
          realizedPnl: 0
        });
    }

    const state = symbolStates.get(apiSymbol);
    state.name = normalizeSecurityName(trade.apiSymbol || trade.symbol, trade.name || state.name, trade.market || state.market, {
      nameSource: trade.nameSource
    });
    state.market = trade.market || state.market;
    state.nameSource = String(trade.nameSource || state.nameSource || "").trim().toLowerCase();

    const qty = Math.max(0, toNumber(trade.quantity));
    const price = toNumber(trade.price);
    const fee = Math.max(0, toNumber(trade.fee));
    const type = String(trade.type || "BUY").toUpperCase();

    if (!qty) {
      continue;
    }

    if (type === "BUY") {
      const totalCostBefore = state.avgCost * state.quantity;
      const totalCostAfter = totalCostBefore + price * qty + fee;
      state.quantity += qty;
      state.avgCost = state.quantity > 0 ? totalCostAfter / state.quantity : 0;
      continue;
    }

    if (type === "SELL") {
      const sellQty = Math.min(qty, state.quantity);
      if (sellQty <= 0) {
        continue;
      }
      const proceeds = price * sellQty - fee;
      const costOut = state.avgCost * sellQty;
      state.realizedPnl += proceeds - costOut;
      state.quantity -= sellQty;

      if (state.quantity <= 0) {
        state.quantity = 0;
        state.avgCost = 0;
      }
    }
  }

  const positions = [];
  let totalMarketValue = 0;
  let totalCost = 0;
  let totalUnrealizedPnl = 0;
  let totalDailyPnl = 0;
  let totalRealizedPnl = 0;

  for (const state of symbolStates.values()) {
    totalRealizedPnl += state.realizedPnl;

    if (state.quantity <= 0) {
      continue;
    }

    const quote = quotes[state.apiSymbol] || {};
    const lastPrice = toNumber(quote.lastPrice) || state.avgCost;
    const previousClose = toNumber(quote.previousClose) || lastPrice;

    const marketValue = state.quantity * lastPrice;
    const holdingCost = state.quantity * state.avgCost;
    const unrealizedPnl = marketValue - holdingCost;
    const dailyPnl = state.quantity * (lastPrice - previousClose);

    totalMarketValue += marketValue;
    totalCost += holdingCost;
    totalUnrealizedPnl += unrealizedPnl;
    totalDailyPnl += dailyPnl;

    positions.push({
      apiSymbol: state.apiSymbol,
      symbol: state.rawSymbol,
      name:
        normalizeSecurityName(state.apiSymbol || state.rawSymbol, state.name || state.apiSymbol, state.market, {
          nameSource: state.nameSource
        }) || state.apiSymbol,
      market: state.market,
      quantity: state.quantity,
      avgCost: state.avgCost,
      lastPrice,
      previousClose,
      marketValue,
      unrealizedPnl,
      unrealizedPct: holdingCost > 0 ? (unrealizedPnl / holdingCost) * 100 : 0,
      dailyPnl,
      dailyPct: previousClose > 0 ? ((lastPrice - previousClose) / previousClose) * 100 : 0,
      quoteAsOf: quote.asOf || null,
      currency: quote.currency || ""
    });
  }

  for (const position of positions) {
    position.weightPct = totalMarketValue > 0 ? (position.marketValue / totalMarketValue) * 100 : 0;
  }

  positions.sort((a, b) => b.marketValue - a.marketValue);

  const previousValue = totalMarketValue - totalDailyPnl;

  return {
    positions,
    summary: {
      totalMarketValue,
      totalCost,
      totalUnrealizedPnl,
      totalUnrealizedPct: totalCost > 0 ? (totalUnrealizedPnl / totalCost) * 100 : 0,
      totalDailyPnl,
      totalDailyPct: previousValue > 0 ? (totalDailyPnl / previousValue) * 100 : 0,
      totalRealizedPnl,
      holdingCount: positions.length,
      trackedCount: symbolStates.size
    }
  };
}

function toMonthKey(inputDate) {
  const date = toDateSafe(inputDate) || new Date();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${date.getFullYear()}-${month}`;
}

function getMonthEnd(inputDate = new Date()) {
  const date = toDateSafe(inputDate) || new Date();
  return new Date(date.getFullYear(), date.getMonth() + 1, 0, 23, 59, 59, 999);
}

function daysBetween(now, target) {
  const millis = target.getTime() - now.getTime();
  return Math.max(0, Math.ceil(millis / (1000 * 60 * 60 * 24)));
}

function buildMonthlyStatus(monthlyUpdates, now = new Date()) {
  const safeUpdates = [...monthlyUpdates].sort((a, b) => {
    const aTime = toDateSafe(a.postedAt || a.createdAt)?.getTime() ?? 0;
    const bTime = toDateSafe(b.postedAt || b.createdAt)?.getTime() ?? 0;
    return bTime - aTime;
  });

  const currentMonth = toMonthKey(now);
  const monthEnd = getMonthEnd(now);
  const daysToMonthEnd = daysBetween(now, monthEnd);
  const inWindow = now.getDate() >= 25;

  const currentMonthUpdates = safeUpdates.filter((item) => item.month === currentMonth);
  const latest = safeUpdates[0] || null;

  if (currentMonthUpdates.length > 0) {
    return {
      level: "done",
      currentMonth,
      daysToMonthEnd,
      updated: true,
      message: `本月（${currentMonth}）已记录 ${currentMonthUpdates.length} 次更新。`,
      latest
    };
  }

  if (inWindow) {
    return {
      level: "watch",
      currentMonth,
      daysToMonthEnd,
      updated: false,
      message: `进入月末跟踪窗口：${currentMonth} 仍未记录更新，建议关注雪球/微博。`,
      latest
    };
  }

  return {
    level: "idle",
    currentMonth,
    daysToMonthEnd,
    updated: false,
    message: `当前不在月末窗口，下次重点关注 ${currentMonth} 月末。`,
    latest
  };
}

module.exports = {
  buildPortfolio,
  buildMonthlyStatus,
  toMonthKey
};
