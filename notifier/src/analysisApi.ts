/**
 * Analysis API 客戶端
 */

import axios, { AxiosError } from 'axios';
import { config } from './config.js';
import logger from './logger.js';
import type { EarningsCallItem, AnalysisResponse } from './types.js';

/**
 * Axios 實例
 */
const apiClient = axios.create({
  baseURL: config.ANALYSIS_API_BASE,
  timeout: 120000, // 2 分鐘（分析可能較久）
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * 取得日期區間的 Earnings Calls
 */
export async function fetchEarningsRange(
  startDate: string,
  endDate: string
): Promise<EarningsCallItem[]> {
  logger.info({ startDate, endDate }, '取得 Earnings Calendar...');

  try {
    const response = await apiClient.get<EarningsCallItem[]>(
      '/api/earnings-calendar/range',
      {
        params: {
          start_date: startDate,
          end_date: endDate,
          min_market_cap: config.MIN_MARKET_CAP,
          refresh: false,
        },
      }
    );

    logger.info({ count: response.data.length }, '取得 Earnings 清單');
    return response.data;
  } catch (error) {
    const axiosError = error as AxiosError;
    logger.error(
      {
        status: axiosError.response?.status,
        message: axiosError.message,
      },
      '取得 Earnings Calendar 失敗'
    );
    throw error;
  }
}

/**
 * 分析單一 Earnings Call
 */
export async function analyzeEarningsCall(
  symbol: string,
  date: string
): Promise<AnalysisResponse> {
  logger.debug({ symbol, date }, '分析中...');

  try {
    const response = await apiClient.post<AnalysisResponse>('/api/analyze', {
      symbol,
      date,
      refresh: false,
    });

    logger.debug(
      {
        symbol,
        prediction: response.data.agentic_result.prediction,
        trade_long: response.data.agentic_result.trade_long,
      },
      '分析完成'
    );

    return response.data;
  } catch (error) {
    const axiosError = error as AxiosError;
    const status = axiosError.response?.status;
    const errorData = axiosError.response?.data as { message?: string; detail?: string } | undefined;

    logger.warn(
      {
        symbol,
        date,
        status,
        message: errorData?.message || errorData?.detail || axiosError.message,
      },
      '分析失敗'
    );

    throw error;
  }
}

/**
 * 判斷錯誤是否為 transcript 尚未取得
 */
export function isTranscriptPendingError(error: unknown): boolean {
  if (!(error instanceof AxiosError)) return false;

  const status = error.response?.status;
  if (status === 404) return true;

  const errorData = error.response?.data as { message?: string; detail?: string; error?: string } | undefined;
  const errorText = JSON.stringify(errorData || '').toLowerCase();

  return (
    errorText.includes('transcript') ||
    errorText.includes('not found') ||
    errorText.includes('no transcript')
  );
}

/**
 * 取得錯誤訊息
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof AxiosError) {
    const errorData = error.response?.data as { message?: string; detail?: string; error?: string } | undefined;
    return (
      errorData?.message ||
      errorData?.detail ||
      errorData?.error ||
      error.message
    );
  }
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
