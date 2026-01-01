/**
 * Earnings Call Analyzer - TypeScript 型別定義
 */

// ============================================
// API 回應型別
// ============================================

/**
 * 單一 Earnings Call 項目（來自 /api/earnings-calendar）
 */
export interface EarningsCallItem {
  symbol: string;
  company: string;
  sector?: string;
  exchange?: string;
  date: string; // YYYY-MM-DD
  eps_estimated?: number | null;
  eps_actual?: number | null;
  market_cap?: number | null;
}

/**
 * 分析結果中的 long_eligible_json
 */
export interface LongEligibleJson {
  DirectionScore?: number;
  [key: string]: unknown;
}

/**
 * Agentic 分析結果
 */
export interface AgenticResult {
  prediction: 'UP' | 'DOWN' | 'UNKNOWN';
  confidence: number; // 0~1
  summary: string;
  reasons: string[];
  trade_long: boolean;
  long_eligible_json?: LongEligibleJson;
}

/**
 * 完整分析回應（來自 POST /api/analyze）
 */
export interface AnalysisResponse {
  symbol: string;
  transcript_date?: string;
  agentic_result: AgenticResult;
}

/**
 * API 錯誤回應
 */
export interface ApiError {
  error: string;
  message?: string;
  detail?: string;
}

// ============================================
// 前端狀態型別
// ============================================

/**
 * 按日期分組的 Earnings Calls
 */
export interface GroupedEarningsCalls {
  date: string; // YYYY-MM-DD
  calls: EarningsCallItem[];
}

/**
 * 分析狀態
 */
export type AnalysisStatus = 'idle' | 'loading' | 'success' | 'error' | 'pending';

/**
 * 分析結果狀態
 */
export interface AnalysisState {
  status: AnalysisStatus;
  data?: AnalysisResponse;
  error?: string;
  symbol?: string;
  date?: string;
}

/**
 * 選中的 Earnings Call
 */
export interface SelectedCall {
  symbol: string;
  company: string;
  date: string;
}

// ============================================
// 設定常數型別
// ============================================

/**
 * 應用程式設定
 */
export interface AppConfig {
  lookbackDays: number;
  defaultMinMarketCap: number;
  maxReasonsToShow: number;
  analysisApiBase: string;
}

// ============================================
// BFF API 請求參數
// ============================================

/**
 * /api/bff/earnings/range 查詢參數
 */
export interface EarningsRangeParams {
  start_date: string;
  end_date: string;
  min_market_cap?: number;
  refresh?: boolean;
}

/**
 * /api/bff/earnings/today 查詢參數
 */
export interface EarningsTodayParams {
  date: string;
  min_market_cap?: number;
}

/**
 * /api/bff/analyze 請求 body
 */
export interface AnalyzeRequest {
  symbol: string;
  date: string;
  refresh?: boolean;
}

// ============================================
// UI 輔助型別
// ============================================

/**
 * 排序選項
 */
export type SortOption = 'market_cap_desc' | 'market_cap_asc' | 'symbol_asc' | 'symbol_desc';

/**
 * 篩選狀態
 */
export interface FilterState {
  searchQuery: string;
  sortBy: SortOption;
}
