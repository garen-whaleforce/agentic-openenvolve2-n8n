/**
 * 型別定義
 */

/**
 * Earnings Call 項目
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
  confidence: number;
  summary: string;
  reasons: string[];
  trade_long: boolean;
  long_eligible_json?: LongEligibleJson;
}

/**
 * 分析回應
 */
export interface AnalysisResponse {
  symbol: string;
  transcript_date?: string;
  agentic_result: AgenticResult;
}

/**
 * 分析狀態
 */
export type AnalysisStatus = 'BUY' | 'NO_ACTION' | 'PENDING' | 'ERROR';

/**
 * 單檔分析結果
 */
export interface SymbolAnalysis {
  symbol: string;
  company: string;
  date: string;
  status: AnalysisStatus;
  confidence?: number;
  prediction?: string;
  reasons?: string[];
  directionScore?: number;
  error?: string;
}

/**
 * 每日掃描結果
 */
export interface DailyScanResult {
  targetDate: string;
  scannedAt: string;
  totalSymbols: number;
  analyzedCount: number;
  buyCount: number;
  noActionCount: number;
  pendingCount: number;
  errorCount: number;
  buyList: SymbolAnalysis[];
  noActionList: SymbolAnalysis[];
  pendingList: SymbolAnalysis[];
  errorList: SymbolAnalysis[];
}

/**
 * LINE 訊息
 */
export interface LineTextMessage {
  type: 'text';
  text: string;
}

/**
 * LINE Push 請求
 */
export interface LinePushRequest {
  to: string;
  messages: LineTextMessage[];
}

/**
 * LINE API 回應
 */
export interface LineApiResponse {
  success: boolean;
  statusCode?: number;
  error?: string;
}
