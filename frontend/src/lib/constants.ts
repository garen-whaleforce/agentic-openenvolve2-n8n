/**
 * 應用程式常數設定
 * 所有可調整參數集中管理於此
 */

/**
 * 往前查詢天數（含選定日期）
 * 例如：LOOKBACK_DAYS = 3 表示 D, D-1, D-2 共三天
 */
export const LOOKBACK_DAYS = 3;

/**
 * 預設最小市值門檻（美元）
 * 可透過環境變數 DEFAULT_MIN_MARKET_CAP 覆寫
 */
export const DEFAULT_MIN_MARKET_CAP = parseInt(
  process.env.NEXT_PUBLIC_DEFAULT_MIN_MARKET_CAP || '1000000000',
  10
);

/**
 * 預設顯示的 reasons 條數
 */
export const MAX_REASONS_TO_SHOW = 3;

/**
 * 分析結果快取時間（毫秒）
 * 用於 SWR revalidation
 */
export const ANALYSIS_CACHE_TIME = 5 * 60 * 1000; // 5 分鐘

/**
 * Earnings 清單快取時間（毫秒）
 */
export const EARNINGS_LIST_CACHE_TIME = 2 * 60 * 1000; // 2 分鐘

/**
 * API 請求超時時間（毫秒）
 */
export const API_TIMEOUT = 120000; // 120 秒（分析可能較久）

/**
 * 美東時區 IANA 名稱
 */
export const EASTERN_TIMEZONE = 'America/New_York';

/**
 * 日期格式
 */
export const DATE_FORMAT = 'yyyy-MM-dd';

/**
 * 市值格式化選項
 */
export const MARKET_CAP_UNITS = [
  { threshold: 1e12, suffix: 'T', divisor: 1e12 },
  { threshold: 1e9, suffix: 'B', divisor: 1e9 },
  { threshold: 1e6, suffix: 'M', divisor: 1e6 },
  { threshold: 1e3, suffix: 'K', divisor: 1e3 },
] as const;
