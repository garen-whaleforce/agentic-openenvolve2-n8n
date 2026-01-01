/**
 * 工具函式
 */

import { DateTime } from 'luxon';
import { EASTERN_TIMEZONE, DATE_FORMAT, MARKET_CAP_UNITS, LOOKBACK_DAYS } from './constants';
import type { EarningsCallItem, GroupedEarningsCalls, SortOption } from './types';

/**
 * 取得美東時區的今天日期（YYYY-MM-DD）
 */
export function getTodayET(): string {
  return DateTime.now().setZone(EASTERN_TIMEZONE).toFormat(DATE_FORMAT);
}

/**
 * 取得美東時區的日期範圍（往前 N 天）
 * @param endDate 結束日期（預設為今天 ET）
 * @param days 往前天數（預設為 LOOKBACK_DAYS）
 * @returns { startDate, endDate }
 */
export function getDateRangeET(
  endDate?: string,
  days: number = LOOKBACK_DAYS
): { startDate: string; endDate: string } {
  const end = endDate
    ? DateTime.fromFormat(endDate, DATE_FORMAT, { zone: EASTERN_TIMEZONE })
    : DateTime.now().setZone(EASTERN_TIMEZONE);

  const start = end.minus({ days: days - 1 });

  return {
    startDate: start.toFormat(DATE_FORMAT),
    endDate: end.toFormat(DATE_FORMAT),
  };
}

/**
 * 格式化日期為人類可讀格式（繁中）
 * @param dateStr YYYY-MM-DD
 * @returns 例如 "1月31日 (週五)"
 */
export function formatDateDisplay(dateStr: string): string {
  const dt = DateTime.fromFormat(dateStr, DATE_FORMAT, { zone: EASTERN_TIMEZONE });
  const weekdays = ['週日', '週一', '週二', '週三', '週四', '週五', '週六'];
  const weekday = weekdays[dt.weekday % 7];
  return `${dt.month}月${dt.day}日 (${weekday})`;
}

/**
 * 格式化市值為人類可讀格式
 * @param marketCap 市值（美元）
 * @returns 例如 "3.0T" 或 "150.5B"
 */
export function formatMarketCap(marketCap: number | null | undefined): string {
  if (marketCap == null) return 'N/A';

  for (const unit of MARKET_CAP_UNITS) {
    if (marketCap >= unit.threshold) {
      const value = marketCap / unit.divisor;
      return `$${value.toFixed(1)}${unit.suffix}`;
    }
  }

  return `$${marketCap.toLocaleString()}`;
}

/**
 * 格式化信心度為百分比
 * @param confidence 0~1 的數值
 * @returns 例如 "78%"
 */
export function formatConfidence(confidence: number): string {
  return `${Math.round(confidence * 100)}%`;
}

/**
 * 將 Earnings Calls 按日期分組
 * @param calls Earnings Call 清單
 * @returns 按日期分組的清單（日期由新到舊）
 */
export function groupCallsByDate(calls: EarningsCallItem[]): GroupedEarningsCalls[] {
  const grouped = new Map<string, EarningsCallItem[]>();

  for (const call of calls) {
    const existing = grouped.get(call.date) || [];
    existing.push(call);
    grouped.set(call.date, existing);
  }

  // 轉換為陣列並按日期排序（由新到舊）
  return Array.from(grouped.entries())
    .map(([date, calls]) => ({ date, calls }))
    .sort((a, b) => b.date.localeCompare(a.date));
}

/**
 * 排序 Earnings Calls
 * @param calls Earnings Call 清單
 * @param sortBy 排序方式
 * @returns 排序後的清單
 */
export function sortCalls(calls: EarningsCallItem[], sortBy: SortOption): EarningsCallItem[] {
  const sorted = [...calls];

  switch (sortBy) {
    case 'market_cap_desc':
      return sorted.sort((a, b) => (b.market_cap || 0) - (a.market_cap || 0));
    case 'market_cap_asc':
      return sorted.sort((a, b) => (a.market_cap || 0) - (b.market_cap || 0));
    case 'symbol_asc':
      return sorted.sort((a, b) => a.symbol.localeCompare(b.symbol));
    case 'symbol_desc':
      return sorted.sort((a, b) => b.symbol.localeCompare(a.symbol));
    default:
      return sorted;
  }
}

/**
 * 篩選 Earnings Calls（依搜尋關鍵字）
 * @param calls Earnings Call 清單
 * @param query 搜尋關鍵字
 * @returns 篩選後的清單
 */
export function filterCalls(calls: EarningsCallItem[], query: string): EarningsCallItem[] {
  if (!query.trim()) return calls;

  const lowerQuery = query.toLowerCase().trim();
  return calls.filter(
    (call) =>
      call.symbol.toLowerCase().includes(lowerQuery) ||
      call.company.toLowerCase().includes(lowerQuery)
  );
}

/**
 * 取得預測結果的顯示文字與樣式
 */
export function getPredictionDisplay(prediction: string): {
  text: string;
  colorClass: string;
  bgClass: string;
} {
  switch (prediction) {
    case 'UP':
      return {
        text: '看漲 ↑',
        colorClass: 'text-success-700',
        bgClass: 'bg-success-50',
      };
    case 'DOWN':
      return {
        text: '看跌 ↓',
        colorClass: 'text-danger-600',
        bgClass: 'bg-danger-50',
      };
    default:
      return {
        text: '不確定',
        colorClass: 'text-gray-600',
        bgClass: 'bg-gray-100',
      };
  }
}

/**
 * 取得交易建議的顯示文字與樣式
 */
export function getTradeRecommendation(tradeLong: boolean): {
  text: string;
  colorClass: string;
  bgClass: string;
  borderClass: string;
} {
  if (tradeLong) {
    return {
      text: 'BUY',
      colorClass: 'text-success-700',
      bgClass: 'bg-success-100',
      borderClass: 'border-success-500',
    };
  }
  return {
    text: '不處理',
    colorClass: 'text-gray-600',
    bgClass: 'bg-gray-100',
    borderClass: 'border-gray-300',
  };
}

/**
 * 判斷錯誤是否為 transcript 尚未取得
 */
export function isTranscriptPendingError(error: string): boolean {
  const pendingKeywords = [
    'transcript',
    'not found',
    'no transcript',
    '找不到',
    'pending',
    '尚未',
  ];
  const lowerError = error.toLowerCase();
  return pendingKeywords.some((keyword) => lowerError.includes(keyword));
}

/**
 * 截斷文字
 */
export function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength) + '...';
}

/**
 * 延遲函式
 */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
