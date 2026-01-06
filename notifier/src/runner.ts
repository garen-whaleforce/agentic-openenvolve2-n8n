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
  fetchAnalyzedCalls,
} from './analysisApi.js';
import { pushMultipleTexts, formatConfidence } from './line.js';
import type {
  EarningsCallItem,
  SymbolAnalysis,
  DailyScanResult,
  AnalysisStatus,
} from './types.js';
import {
  loadQueue,
  addToQueue,
  removeFromQueue,
  updateRetryCount,
  cleanupExpiredItems,
  getQueueStats,
  type PendingItem,
} from './pendingQueue.js';

/**
 * 延遲函式
 */
function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 掃描選項
 */
export interface ScanOptions {
  /** 指定結束日期 (YYYY-MM-DD)，預設為昨天 */
  endDate?: string;
  /** 回顧天數，預設使用 config.LOOKBACK_DAYS */
  lookbackDays?: number;
  /** 是否跳過去重檢查（強制重新分析） */
  skipDedup?: boolean;
}

/**
 * 計算日期範圍
 * @param options 掃描選項
 * @param useOffset 是否使用 SCAN_OFFSET_DAYS 偏移（主掃描用）
 */
function getDateRange(
  options?: ScanOptions,
  useOffset: boolean = true
): { startDate: string; endDate: string } {
  const lookbackDays = options?.lookbackDays ?? config.LOOKBACK_DAYS;
  const offsetDays = useOffset ? config.SCAN_OFFSET_DAYS : 0;

  let endDateTime: DateTime;
  if (options?.endDate) {
    endDateTime = DateTime.fromISO(options.endDate, { zone: EASTERN_TIMEZONE });
  } else {
    const now = DateTime.now().setZone(EASTERN_TIMEZONE);
    // 主掃描：掃描 (今天 - offsetDays) 之前的資料
    // 例如 offsetDays=3，lookbackDays=7，則掃描 3-10 天前
    endDateTime = now.minus({ days: 1 + offsetDays });
  }

  const startDateTime = endDateTime.minus({ days: lookbackDays - 1 });

  return {
    startDate: startDateTime.toFormat(DATE_FORMAT),
    endDate: endDateTime.toFormat(DATE_FORMAT),
  };
}

/**
 * 封閉式基金關鍵字列表（這些通常沒有 earnings call transcript）
 */
const CLOSED_END_FUND_KEYWORDS = [
  'fund',
  'income fund',
  'municipal',
  'preferred',
  'opportunities fund',
  'credit fund',
  'value fund',
];

/**
 * 判斷是否為封閉式基金（通常沒有 transcript）
 */
function isClosedEndFund(item: EarningsCallItem): boolean {
  const companyLower = (item.company || '').toLowerCase();
  const sectorLower = (item.sector || '').toLowerCase();

  // 檢查是否為金融服務業的基金類型
  if (sectorLower === 'financial services') {
    for (const keyword of CLOSED_END_FUND_KEYWORDS) {
      if (companyLower.includes(keyword)) {
        return true;
      }
    }
  }

  // Nuveen 系列基金
  if (companyLower.startsWith('nuveen ')) {
    return true;
  }

  // Abrdn 系列基金
  if (companyLower.startsWith('abrdn ')) {
    return true;
  }

  // First Trust 基金
  if (companyLower.includes('first trust') && companyLower.includes('fund')) {
    return true;
  }

  return false;
}

/**
 * 過濾出尚未分析的 Earnings Calls
 * @param calls 所有 Earnings Calls
 * @param analyzedSet 已分析過的 symbol+date 集合
 * @param excludeClosedEndFunds 是否排除封閉式基金
 *
 * 排序邏輯：
 * 1. 先按日期降序（最新優先）
 * 2. 同日期按市值降序
 *
 * 注意：不再限制數量，會掃描所有未分析的，每 BATCH_SIZE 個推送一次
 */
function filterNewCalls(
  calls: EarningsCallItem[],
  analyzedSet: Set<string>,
  excludeClosedEndFunds: boolean = true
): EarningsCallItem[] {
  let filtered = calls.filter((c) => !analyzedSet.has(`${c.symbol}:${c.date}`));

  // 排除封閉式基金
  if (excludeClosedEndFunds) {
    const beforeCount = filtered.length;
    filtered = filtered.filter((c) => !isClosedEndFund(c));
    const excludedCount = beforeCount - filtered.length;
    if (excludedCount > 0) {
      logger.info({ excludedCount }, '排除封閉式基金（通常無 transcript）');
    }
  }

  return filtered
    .sort((a, b) => {
      // 先按日期降序（最新優先）
      const dateCompare = b.date.localeCompare(a.date);
      if (dateCompare !== 0) return dateCompare;
      // 同日期按市值降序
      return (b.market_cap || 0) - (a.market_cap || 0);
    })
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
 * 掃描過去 LOOKBACK_DAYS 天的 earnings，只分析尚未分析過的新 transcript
 * @param options 掃描選項（可指定日期範圍）
 */
export async function runDailyScan(options?: ScanOptions): Promise<DailyScanResult | null> {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const scannedAt = now.toFormat('yyyy-MM-dd HH:mm:ss');

  logger.info('========================================');
  logger.info({ time: scannedAt, options }, '開始每日掃描');

  // 1. 計算日期範圍
  const { startDate, endDate } = getDateRange(options);
  const lookbackDays = options?.lookbackDays ?? config.LOOKBACK_DAYS;
  logger.info({ startDate, endDate, lookbackDays }, '日期範圍');

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

  logger.info({ total: allCalls.length }, '符合市值條件的 Earnings Calls');

  // 3. 取得已分析過的記錄（除非 skipDedup）
  let analyzedSet: Set<string>;
  if (options?.skipDedup) {
    analyzedSet = new Set();
    logger.info('跳過去重檢查（強制重新分析）');
  } else {
    const analyzedCalls = await fetchAnalyzedCalls(startDate, endDate);
    analyzedSet = new Set(
      analyzedCalls.map((c) => `${c.symbol}:${c.date}`)
    );
    logger.info({ analyzedCount: analyzedSet.size }, '已分析過的記錄');
  }

  // 4. 過濾出尚未分析的新 calls
  const newCalls = filterNewCalls(allCalls, analyzedSet);

  if (newCalls.length === 0) {
    logger.info('沒有新的 Earnings Call 需要分析');
    // 不推播訊息，靜默結束
    logger.info('========================================');
    return null;
  }

  logger.info(
    { newCount: newCalls.length, symbols: newCalls.map((c) => c.symbol) },
    '待分析的新 Earnings Calls'
  );

  // 5. 推播清單訊息
  const tickerPreview = newCalls.slice(0, 20).map((c) => `${c.symbol}(${c.date})`).join(', ');
  const listMessage =
    `📅 Earnings Call 新增掃描\n\n` +
    `美東時間：${scannedAt}\n` +
    `查詢範圍：${startDate} ~ ${endDate}\n` +
    `新增待分析：${newCalls.length} 檔\n\n` +
    `Tickers：${tickerPreview}${newCalls.length > 20 ? '...' : ''}\n\n` +
    `開始分析（每 ${config.BATCH_SIZE} 檔推送一次）...`;

  await pushMultipleTexts([listMessage]);

  // 6. 逐檔分析，每 BATCH_SIZE 個推送一次
  const allResults: SymbolAnalysis[] = [];
  let batchResults: SymbolAnalysis[] = [];
  let batchNumber = 0;

  for (let i = 0; i < newCalls.length; i++) {
    const item = newCalls[i]!;
    logger.info(
      { index: i + 1, total: newCalls.length, symbol: item.symbol, date: item.date },
      '分析中'
    );

    const analysis = await analyzeSymbol(item);
    allResults.push(analysis);
    batchResults.push(analysis);

    // 每 BATCH_SIZE 個或最後一個時推送
    if (batchResults.length >= config.BATCH_SIZE || i === newCalls.length - 1) {
      batchNumber++;
      const batchSuccessful = batchResults.filter(
        (r) => r.status === 'BUY' || r.status === 'NO_ACTION'
      );

      if (batchSuccessful.length > 0) {
        const batchScanResult = createBatchResult(
          batchResults,
          scannedAt,
          batchNumber,
          i + 1,
          newCalls.length
        );
        const batchMessages = formatBatchResultMessages(batchScanResult);
        await pushMultipleTexts(batchMessages);
      } else {
        // 這批全部都是 PENDING 或 ERROR
        const pendingCount = batchResults.filter((r) => r.status === 'PENDING').length;
        const errorCount = batchResults.filter((r) => r.status === 'ERROR').length;
        logger.info(
          { batchNumber, pending: pendingCount, error: errorCount },
          '這批全部都是 PENDING/ERROR，不推播'
        );
      }

      batchResults = [];
    }

    // 延遲避免 rate limit
    if (i < newCalls.length - 1) {
      await delay(config.REQUEST_DELAY_MS);
    }
  }

  // 7. 分類最終結果
  const buyList = allResults.filter((r) => r.status === 'BUY');
  const noActionList = allResults.filter((r) => r.status === 'NO_ACTION');
  const pendingList = allResults.filter((r) => r.status === 'PENDING');
  const errorList = allResults.filter((r) => r.status === 'ERROR');

  // 使用最新日期作為 targetDate
  const targetDate = newCalls[0]?.date || endDate;

  const scanResult: DailyScanResult = {
    targetDate,
    scannedAt,
    totalSymbols: newCalls.length,
    analyzedCount: allResults.length,
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
    '全部分析完成'
  );

  // 8. 將 PENDING 項目加入待分析佇列
  if (pendingList.length > 0) {
    const pendingCalls: EarningsCallItem[] = pendingList.map((p) => ({
      symbol: p.symbol,
      company: p.company,
      date: p.date,
    }));
    const addedCount = addToQueue(pendingCalls);
    logger.info({ addedCount, pendingCount: pendingList.length }, 'PENDING 項目加入待分析佇列');
  }

  // 9. 推播最終摘要
  const queueStats = getQueueStats();
  let finalSummary =
    `📊 Earnings Call 掃描完成\n\n` +
    `查詢範圍：${startDate} ~ ${endDate}\n` +
    `（偏移 ${config.SCAN_OFFSET_DAYS} 天，確保 transcript 已上傳）\n` +
    `總共分析：${allResults.length} 檔\n\n` +
    `✅ BUY：${buyList.length}\n` +
    `⚪ NO ACTION：${noActionList.length}\n` +
    `⏳ PENDING：${pendingList.length}\n` +
    `❌ ERROR：${errorList.length}`;

  if (queueStats.totalCount > 0) {
    finalSummary += `\n\n📋 待分析佇列：${queueStats.totalCount} 檔`;
  }

  await pushMultipleTexts([finalSummary]);

  logger.info('========================================');

  return scanResult;
}

/**
 * 批次結果結構
 */
interface BatchResult {
  batchNumber: number;
  currentIndex: number;
  totalCount: number;
  scannedAt: string;
  results: SymbolAnalysis[];
  buyList: SymbolAnalysis[];
  noActionList: SymbolAnalysis[];
  pendingList: SymbolAnalysis[];
  errorList: SymbolAnalysis[];
}

/**
 * 建立批次結果
 */
function createBatchResult(
  results: SymbolAnalysis[],
  scannedAt: string,
  batchNumber: number,
  currentIndex: number,
  totalCount: number
): BatchResult {
  return {
    batchNumber,
    currentIndex,
    totalCount,
    scannedAt,
    results,
    buyList: results.filter((r) => r.status === 'BUY'),
    noActionList: results.filter((r) => r.status === 'NO_ACTION'),
    pendingList: results.filter((r) => r.status === 'PENDING'),
    errorList: results.filter((r) => r.status === 'ERROR'),
  };
}

/**
 * 格式化批次結果訊息
 */
function formatBatchResultMessages(batch: BatchResult): string[] {
  const messages: string[] = [];

  let summary =
    `📊 批次 #${batch.batchNumber} 分析結果\n` +
    `進度：${batch.currentIndex}/${batch.totalCount}\n\n` +
    `✅ BUY：${batch.buyList.length}\n` +
    `⚪ NO ACTION：${batch.noActionList.length}\n` +
    `⏳ PENDING：${batch.pendingList.length}\n` +
    `❌ ERROR：${batch.errorList.length}`;

  // BUY 清單
  if (batch.buyList.length > 0) {
    summary += `\n\n━━━━━━━━━━━━━━━━\n✅ BUY 建議\n━━━━━━━━━━━━━━━━`;

    for (const item of batch.buyList) {
      summary += `\n\n📈 ${item.symbol} (${item.date})`;
      if (item.confidence != null) {
        summary += ` ${formatConfidence(item.confidence)}`;
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
            reason.length > 80 ? reason.slice(0, 80) + '...' : reason;
          summary += `\n• ${truncated}`;
        }
      }
    }
  }

  // NO ACTION 清單（簡短顯示）
  if (batch.noActionList.length > 0) {
    const noActionSymbols = batch.noActionList.map((r) => r.symbol).join(', ');
    summary += `\n\n⚪ NO ACTION: ${noActionSymbols}`;
  }

  messages.push(summary);

  return messages;
}

/**
 * 格式化結果訊息（完整版，供外部使用）
 */
export function formatResultMessages(result: DailyScanResult): string[] {
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

/**
 * 重試佇列結果
 */
export interface RetryQueueResult {
  processedCount: number;
  successCount: number;
  stillPendingCount: number;
  expiredCount: number;
  buyList: SymbolAnalysis[];
  noActionList: SymbolAnalysis[];
}

/**
 * 執行待分析佇列重試
 * 檢查佇列中的項目，嘗試分析那些可能已經有 transcript 的
 */
export async function runRetryQueue(): Promise<RetryQueueResult | null> {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const scannedAt = now.toFormat('yyyy-MM-dd HH:mm:ss');

  logger.info('========================================');
  logger.info({ time: scannedAt }, '開始重試待分析佇列');

  // 1. 清理過期項目
  const expiredCount = cleanupExpiredItems();

  // 2. 載入佇列
  const queue = loadQueue();
  if (queue.length === 0) {
    logger.info('待分析佇列為空，跳過重試');
    logger.info('========================================');
    return null;
  }

  logger.info({ queueSize: queue.length }, '待分析佇列項目數');

  // 3. 逐個重試
  const successItems: SymbolAnalysis[] = [];
  const stillPendingItems: PendingItem[] = [];

  for (const item of queue) {
    logger.info(
      { symbol: item.symbol, date: item.date, retryCount: item.retryCount },
      '重試分析'
    );

    const analysis = await analyzeSymbol(item);

    if (analysis.status === 'PENDING') {
      // 仍然沒有 transcript
      updateRetryCount(item.symbol, item.date);
      stillPendingItems.push(item);
    } else {
      // 成功分析（BUY, NO_ACTION, 或 ERROR）
      successItems.push(analysis);
    }

    // 延遲避免 rate limit
    await delay(config.REQUEST_DELAY_MS);
  }

  // 4. 從佇列移除成功分析的項目
  if (successItems.length > 0) {
    removeFromQueue(
      successItems.map((s) => ({ symbol: s.symbol, date: s.date }))
    );
  }

  // 5. 分類結果
  const buyList = successItems.filter((r) => r.status === 'BUY');
  const noActionList = successItems.filter((r) => r.status === 'NO_ACTION');

  const result: RetryQueueResult = {
    processedCount: queue.length,
    successCount: successItems.length,
    stillPendingCount: stillPendingItems.length,
    expiredCount,
    buyList,
    noActionList,
  };

  logger.info(
    {
      processed: result.processedCount,
      success: result.successCount,
      stillPending: result.stillPendingCount,
      expired: result.expiredCount,
      buy: buyList.length,
    },
    '重試佇列處理完成'
  );

  // 6. 如果有 BUY 訊號，推播通知
  if (buyList.length > 0) {
    let message =
      `🔄 重試佇列分析結果\n\n` +
      `處理：${result.processedCount} 檔\n` +
      `成功：${result.successCount} 檔\n` +
      `仍等待：${result.stillPendingCount} 檔\n\n` +
      `✅ BUY：${buyList.length}\n` +
      `⚪ NO ACTION：${noActionList.length}`;

    message += `\n\n━━━━━━━━━━━━━━━━\n✅ BUY 建議\n━━━━━━━━━━━━━━━━`;

    for (const item of buyList) {
      message += `\n\n📈 ${item.symbol} (${item.date})`;
      if (item.confidence != null) {
        message += ` ${formatConfidence(item.confidence)}`;
      }
      if (item.directionScore != null) {
        message += ` [D${item.directionScore}]`;
      }
      message += `\n${item.company}`;

      if (item.reasons && item.reasons.length > 0) {
        const topReasons = item.reasons.slice(0, 2);
        for (const reason of topReasons) {
          const truncated =
            reason.length > 80 ? reason.slice(0, 80) + '...' : reason;
          message += `\n• ${truncated}`;
        }
      }
    }

    await pushMultipleTexts([message]);
  } else if (result.successCount > 0) {
    // 有成功分析但沒有 BUY
    const message =
      `🔄 重試佇列分析結果\n\n` +
      `處理：${result.processedCount} 檔\n` +
      `成功：${result.successCount} 檔\n` +
      `仍等待：${result.stillPendingCount} 檔\n\n` +
      `⚪ 無 BUY 訊號`;

    await pushMultipleTexts([message]);
  }

  logger.info('========================================');

  return result;
}
