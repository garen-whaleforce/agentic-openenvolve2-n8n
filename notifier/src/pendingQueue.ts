/**
 * 待分析佇列管理
 * 用於存儲因 transcript 尚未上傳而暫時無法分析的 Earnings Calls
 */

import fs from 'fs';
import path from 'path';
import { DateTime } from 'luxon';
import { config, EASTERN_TIMEZONE, DATE_FORMAT } from './config.js';
import logger from './logger.js';
import type { EarningsCallItem } from './types.js';

/**
 * 佇列項目
 */
export interface PendingItem extends EarningsCallItem {
  /** 加入佇列的時間 */
  addedAt: string;
  /** 重試次數 */
  retryCount: number;
  /** 最後重試時間 */
  lastRetryAt?: string;
}

/**
 * 佇列資料結構
 */
interface QueueData {
  version: number;
  updatedAt: string;
  items: PendingItem[];
}

const QUEUE_VERSION = 1;
const QUEUE_FILENAME = 'pending-queue.json';

/**
 * 取得佇列檔案路徑
 */
function getQueueFilePath(): string {
  return path.join(config.DATA_DIR, QUEUE_FILENAME);
}

/**
 * 確保資料目錄存在
 */
function ensureDataDir(): void {
  if (!fs.existsSync(config.DATA_DIR)) {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
    logger.info({ dir: config.DATA_DIR }, '建立資料目錄');
  }
}

/**
 * 讀取佇列
 */
export function loadQueue(): PendingItem[] {
  const filePath = getQueueFilePath();

  if (!fs.existsSync(filePath)) {
    return [];
  }

  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const data: QueueData = JSON.parse(content);
    logger.debug({ count: data.items.length }, '載入待分析佇列');
    return data.items;
  } catch (error) {
    logger.error({ error, filePath }, '讀取待分析佇列失敗');
    return [];
  }
}

/**
 * 儲存佇列
 */
export function saveQueue(items: PendingItem[]): void {
  ensureDataDir();

  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const data: QueueData = {
    version: QUEUE_VERSION,
    updatedAt: now.toISO() || now.toFormat('yyyy-MM-dd HH:mm:ss'),
    items,
  };

  const filePath = getQueueFilePath();
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  logger.debug({ count: items.length, filePath }, '儲存待分析佇列');
}

/**
 * 新增項目到佇列
 */
export function addToQueue(calls: EarningsCallItem[]): number {
  const existingQueue = loadQueue();
  const existingKeys = new Set(
    existingQueue.map((item) => `${item.symbol}:${item.date}`)
  );

  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const nowStr = now.toFormat('yyyy-MM-dd HH:mm:ss');

  let addedCount = 0;
  for (const call of calls) {
    const key = `${call.symbol}:${call.date}`;
    if (!existingKeys.has(key)) {
      existingQueue.push({
        ...call,
        addedAt: nowStr,
        retryCount: 0,
      });
      addedCount++;
    }
  }

  if (addedCount > 0) {
    saveQueue(existingQueue);
    logger.info({ addedCount, totalCount: existingQueue.length }, '新增項目到待分析佇列');
  }

  return addedCount;
}

/**
 * 從佇列移除項目
 */
export function removeFromQueue(symbols: Array<{ symbol: string; date: string }>): number {
  const queue = loadQueue();
  const keysToRemove = new Set(
    symbols.map((s) => `${s.symbol}:${s.date}`)
  );

  const newQueue = queue.filter(
    (item) => !keysToRemove.has(`${item.symbol}:${item.date}`)
  );

  const removedCount = queue.length - newQueue.length;
  if (removedCount > 0) {
    saveQueue(newQueue);
    logger.info({ removedCount, remainingCount: newQueue.length }, '從待分析佇列移除項目');
  }

  return removedCount;
}

/**
 * 更新項目重試次數
 */
export function updateRetryCount(symbol: string, date: string): void {
  const queue = loadQueue();
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const nowStr = now.toFormat('yyyy-MM-dd HH:mm:ss');

  for (const item of queue) {
    if (item.symbol === symbol && item.date === date) {
      item.retryCount++;
      item.lastRetryAt = nowStr;
      break;
    }
  }

  saveQueue(queue);
}

/**
 * 清理過期項目
 * 移除超過 RETRY_MAX_DAYS 天的項目
 */
export function cleanupExpiredItems(): number {
  const queue = loadQueue();
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);
  const cutoffDate = now.minus({ days: config.RETRY_MAX_DAYS }).toFormat(DATE_FORMAT);

  const newQueue = queue.filter((item) => {
    // 保留 earnings date 在 cutoff 之後的項目
    return item.date >= cutoffDate;
  });

  const removedCount = queue.length - newQueue.length;
  if (removedCount > 0) {
    saveQueue(newQueue);
    logger.info(
      { removedCount, cutoffDate, remainingCount: newQueue.length },
      '清理過期的待分析項目'
    );
  }

  return removedCount;
}

/**
 * 取得佇列統計
 */
export function getQueueStats(): {
  totalCount: number;
  oldestDate: string | null;
  newestDate: string | null;
  avgRetryCount: number;
} {
  const queue = loadQueue();

  if (queue.length === 0) {
    return {
      totalCount: 0,
      oldestDate: null,
      newestDate: null,
      avgRetryCount: 0,
    };
  }

  const dates = queue.map((item) => item.date).sort();
  const totalRetries = queue.reduce((sum, item) => sum + item.retryCount, 0);

  return {
    totalCount: queue.length,
    oldestDate: dates[0] || null,
    newestDate: dates[dates.length - 1] || null,
    avgRetryCount: totalRetries / queue.length,
  };
}
