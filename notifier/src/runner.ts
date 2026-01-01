/**
 * 每日掃描主流程
 */

import { DateTime } from 'luxon';
import { config, EASTERN_TIMEZONE, DATE_FORMAT } from './config.js';
import logger from './logger.js';
import {
  fetchEarningsRange,
  analyzeEarningsCall,
  isTranscriptPendingError,
  getErrorMessage,
} from './analysisApi.js';
import { pushMultipleTexts, formatConfidence } from './line.js';
import type {
  EarningsCallItem,
  SymbolAnalysis,
  DailyScanResult,
  AnalysisStatus,
} from './types.js';

/**
 * 延遲函式
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 計算日期範圍
 */
function getDateRange(): { startDate: string; endDate: string } {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const yesterday = now.minus({ days: 1 });
  const startDate = yesterday.minus({ days: config.LOOKBACK_DAYS - 1 });

  return {
    startDate: startDate.toFormat(DATE_FORMAT),
    endDate: yesterday.toFormat(DATE_FORMAT),
  };
}

/**
 * 取得目標日期（最新有資料的日期）
 */
function getTargetDate(calls: EarningsCallItem[]): string | null {
  if (calls.length === 0) return null;

  // 依日期分組並找最大日期
  const dates = [...new Set(calls.map((c) => c.date))];
  dates.sort((a, b) => b.localeCompare(a)); // 降序

  return dates[0] ?? null;
}

/**
 * 取得目標日期的 Earnings Calls
 */
function getTargetCalls(
  calls: EarningsCallItem[],
  targetDate: string
): EarningsCallItem[] {
  return calls
    .filter((c) => c.date === targetDate)
    .sort((a, b) => (b.market_cap || 0) - (a.market_cap || 0))
    .slice(0, config.MAX_SYMBOLS);
}

/**
 * 分析單檔
 */
async function analyzeSymbol(
  item: EarningsCallItem
): Promise<SymbolAnalysis> {
  try {
    const result = await analyzeEarningsCall(item.symbol, item.date);
    const { agentic_result } = result;

    let status: AnalysisStatus = 'NO_ACTION';
    if (agentic_result.trade_long === true) {
      status = 'BUY';
    }

    return {
      symbol: item.symbol,
      company: item.company,
      date: item.date,
      status,
      confidence: agentic_result.confidence,
      prediction: agentic_result.prediction,
      reasons: agentic_result.reasons,
      directionScore: agentic_result.long_eligible_json?.DirectionScore,
    };
  } catch (error) {
    const isPending = isTranscriptPendingError(error);
    const errorMsg = getErrorMessage(error);

    return {
      symbol: item.symbol,
      company: item.company,
      date: item.date,
      status: isPending ? 'PENDING' : 'ERROR',
      error: errorMsg,
    };
  }
}

/**
 * 執行每日掃描
 */
export async function runDailyScan(): Promise<DailyScanResult | null> {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const scannedAt = now.toFormat('yyyy-MM-dd HH:mm:ss');

  logger.info('========================================');
  logger.info({ time: scannedAt }, '開始每日掃描');

  // 1. 計算日期範圍
  const { startDate, endDate } = getDateRange();
  logger.info({ startDate, endDate }, '日期範圍');

  // 2. 取得 Earnings 清單
  let allCalls: EarningsCallItem[];
  try {
    allCalls = await fetchEarningsRange(startDate, endDate);
  } catch (error) {
    logger.error({ error: getErrorMessage(error) }, '取得 Earnings 清單失敗');
    await pushMultipleTexts([
      `❌ Earnings Call Notifier 錯誤\n\n` +
        `美東時間：${scannedAt}\n` +
        `錯誤：無法取得 Earnings 清單\n` +
        `${getErrorMessage(error)}`,
    ]);
    return null;
  }

  // 3. 找目標日期
  const targetDate = getTargetDate(allCalls);
  if (!targetDate) {
    logger.warn('沒有找到任何 Earnings Call');
    await pushMultipleTexts([
      `📅 Earnings Call Notifier\n\n` +
        `美東時間：${scannedAt}\n` +
        `查詢範圍：${startDate} ~ ${endDate}\n\n` +
        `❌ 這段期間沒有符合條件的 Earnings Call`,
    ]);
    return null;
  }

  // 4. 取得目標日期的清單
  const targetCalls = getTargetCalls(allCalls, targetDate);
  logger.info(
    { targetDate, count: targetCalls.length },
    '目標日期 Earnings Calls'
  );

  // 5. 推播清單訊息
  const tickerPreview = targetCalls.map((c) => c.symbol).join(', ');
  const listMessage =
    `📅 Earnings Call 清單\n\n` +
    `美東時間：${scannedAt}\n` +
    `目標日期：${targetDate}\n` +
    `符合條件：${targetCalls.length} 檔\n\n` +
    `Tickers：${tickerPreview}\n\n` +
    `即將分析前 ${config.MAX_SYMBOLS} 檔...`;

  await pushMultipleTexts([listMessage]);

  // 6. 逐檔分析
  const results: SymbolAnalysis[] = [];
  for (let i = 0; i < targetCalls.length; i++) {
    const item = targetCalls[i]!;
    logger.info(
      { index: i + 1, total: targetCalls.length, symbol: item.symbol },
      '分析中'
    );

    const analysis = await analyzeSymbol(item);
    results.push(analysis);

    // 延遲避免 rate limit
    if (i < targetCalls.length - 1) {
      await delay(config.REQUEST_DELAY_MS);
    }
  }

  // 7. 分類結果
  const buyList = results.filter((r) => r.status === 'BUY');
  const noActionList = results.filter((r) => r.status === 'NO_ACTION');
  const pendingList = results.filter((r) => r.status === 'PENDING');
  const errorList = results.filter((r) => r.status === 'ERROR');

  const scanResult: DailyScanResult = {
    targetDate,
    scannedAt,
    totalSymbols: targetCalls.length,
    analyzedCount: results.length,
    buyCount: buyList.length,
    noActionCount: noActionList.length,
    pendingCount: pendingList.length,
    errorCount: errorList.length,
    buyList,
    noActionList,
    pendingList,
    errorList,
  };

  logger.info(
    {
      buy: buyList.length,
      noAction: noActionList.length,
      pending: pendingList.length,
      error: errorList.length,
    },
    '分析完成'
  );

  // 8. 推播結果訊息
  const resultMessages = formatResultMessages(scanResult);
  await pushMultipleTexts(resultMessages);

  logger.info('========================================');

  return scanResult;
}

/**
 * 格式化結果訊息
 */
function formatResultMessages(result: DailyScanResult): string[] {
  const messages: string[] = [];

  // 摘要訊息
  let summary =
    `📊 Earnings Call 分析結果\n\n` +
    `目標日期：${result.targetDate}\n` +
    `分析時間：${result.scannedAt}\n` +
    `分析檔數：${result.analyzedCount}\n\n` +
    `✅ BUY：${result.buyCount}\n` +
    `⚪ NO ACTION：${result.noActionCount}\n` +
    `⏳ PENDING：${result.pendingCount}\n` +
    `❌ ERROR：${result.errorCount}`;

  // BUY 清單
  if (result.buyList.length > 0) {
    summary += `\n\n━━━━━━━━━━━━━━━━\n✅ BUY 建議清單\n━━━━━━━━━━━━━━━━`;

    for (const item of result.buyList) {
      summary += `\n\n📈 ${item.symbol}`;
      if (item.confidence != null) {
        summary += ` (${formatConfidence(item.confidence)})`;
      }
      if (item.directionScore != null) {
        summary += ` [D${item.directionScore}]`;
      }
      summary += `\n${item.company}`;

      // 顯示前 2 條理由
      if (item.reasons && item.reasons.length > 0) {
        const topReasons = item.reasons.slice(0, 2);
        for (const reason of topReasons) {
          const truncated =
            reason.length > 100 ? reason.slice(0, 100) + '...' : reason;
          summary += `\n• ${truncated}`;
        }
      }
    }
  }

  messages.push(summary);

  // PENDING 清單（如果有）
  if (result.pendingList.length > 0) {
    let pendingMsg = `⏳ PENDING 清單（尚未取得 Transcript）\n`;
    for (const item of result.pendingList) {
      pendingMsg += `\n• ${item.symbol}`;
      if (item.error) {
        const shortError =
          item.error.length > 50 ? item.error.slice(0, 50) + '...' : item.error;
        pendingMsg += `：${shortError}`;
      }
    }
    messages.push(pendingMsg);
  }

  // ERROR 清單（如果有）
  if (result.errorList.length > 0) {
    let errorMsg = `❌ ERROR 清單\n`;
    for (const item of result.errorList) {
      errorMsg += `\n• ${item.symbol}`;
      if (item.error) {
        const shortError =
          item.error.length > 50 ? item.error.slice(0, 50) + '...' : item.error;
        errorMsg += `：${shortError}`;
      }
    }
    messages.push(errorMsg);
  }

  // 風險提示
  messages.push(
    `⚠️ 以上分析結果僅供參考，非投資建議。\n` +
      `策略勝率約 86%，請自行評估風險。`
  );

  return messages;
}
