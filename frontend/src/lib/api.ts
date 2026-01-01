/**
 * API 呼叫封裝
 */

import type {
  EarningsCallItem,
  AnalysisResponse,
  EarningsRangeParams,
  AnalyzeRequest,
} from './types';
import { DEFAULT_MIN_MARKET_CAP } from './constants';

/**
 * API 錯誤類別
 */
export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public code?: string
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/**
 * 通用 fetch 包裝
 */
async function apiFetch<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    let errorData: { error?: string; message?: string; code?: string } = {};
    try {
      errorData = await response.json();
    } catch {
      errorData = { message: await response.text() };
    }

    throw new ApiError(
      errorData.message || errorData.error || `HTTP ${response.status}`,
      response.status,
      errorData.error
    );
  }

  return response.json();
}

/**
 * 取得日期區間的 Earnings Calls
 */
export async function fetchEarningsRange(
  params: EarningsRangeParams
): Promise<EarningsCallItem[]> {
  const searchParams = new URLSearchParams({
    start_date: params.start_date,
    end_date: params.end_date,
    min_market_cap: (params.min_market_cap ?? DEFAULT_MIN_MARKET_CAP).toString(),
    refresh: (params.refresh ?? false).toString(),
  });

  return apiFetch<EarningsCallItem[]>(`/api/bff/earnings/range?${searchParams}`);
}

/**
 * 取得單日的 Earnings Calls
 */
export async function fetchEarningsToday(
  date: string,
  minMarketCap?: number
): Promise<EarningsCallItem[]> {
  const searchParams = new URLSearchParams({
    date,
    min_market_cap: (minMarketCap ?? DEFAULT_MIN_MARKET_CAP).toString(),
  });

  return apiFetch<EarningsCallItem[]>(`/api/bff/earnings/today?${searchParams}`);
}

/**
 * 執行 Earnings Call 分析
 */
export async function analyzeEarningsCall(
  params: AnalyzeRequest
): Promise<AnalysisResponse> {
  return apiFetch<AnalysisResponse>('/api/bff/analyze', {
    method: 'POST',
    body: JSON.stringify(params),
  });
}

// ============================================
// 分析結果快取
// ============================================

/**
 * 快取 key 產生器
 */
function getAnalysisCacheKey(symbol: string, date: string): string {
  return `analysis:${symbol.toUpperCase()}:${date}`;
}

/**
 * 記憶體快取 Map
 */
const analysisCache = new Map<string, { data: AnalysisResponse; timestamp: number }>();

/**
 * 快取有效期（毫秒）
 */
const CACHE_TTL = 5 * 60 * 1000; // 5 分鐘

/**
 * 檢查快取是否有效
 */
function isCacheValid(timestamp: number): boolean {
  return Date.now() - timestamp < CACHE_TTL;
}

/**
 * 取得快取的分析結果
 */
export function getCachedAnalysis(symbol: string, date: string): AnalysisResponse | null {
  const key = getAnalysisCacheKey(symbol, date);
  const cached = analysisCache.get(key);

  if (cached && isCacheValid(cached.timestamp)) {
    return cached.data;
  }

  // 清除過期快取
  if (cached) {
    analysisCache.delete(key);
  }

  return null;
}

/**
 * 設定分析結果快取
 */
export function setCachedAnalysis(symbol: string, date: string, data: AnalysisResponse): void {
  const key = getAnalysisCacheKey(symbol, date);
  analysisCache.set(key, { data, timestamp: Date.now() });
}

/**
 * 帶快取的分析呼叫
 */
export async function analyzeWithCache(
  symbol: string,
  date: string,
  forceRefresh = false
): Promise<AnalysisResponse> {
  // 檢查快取（除非強制更新）
  if (!forceRefresh) {
    const cached = getCachedAnalysis(symbol, date);
    if (cached) {
      console.log(`[Cache Hit] ${symbol} @ ${date}`);
      return cached;
    }
  }

  // 呼叫 API
  const result = await analyzeEarningsCall({
    symbol,
    date,
    refresh: forceRefresh,
  });

  // 存入快取
  setCachedAnalysis(symbol, date, result);

  return result;
}

/**
 * 清除所有快取
 */
export function clearAnalysisCache(): void {
  analysisCache.clear();
}
