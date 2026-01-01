/**
 * 環境變數設定與驗證
 */

import { z } from 'zod';
import dotenv from 'dotenv';

// 載入 .env 檔案
dotenv.config();

/**
 * 環境變數 Schema
 */
const envSchema = z.object({
  // 必填
  ANALYSIS_API_BASE: z
    .string()
    .url('ANALYSIS_API_BASE 必須是有效的 URL')
    .min(1, 'ANALYSIS_API_BASE 為必填'),
  LINE_CHANNEL_ACCESS_TOKEN: z
    .string()
    .min(1, 'LINE_CHANNEL_ACCESS_TOKEN 為必填'),
  LINE_TO: z
    .string()
    .min(1, 'LINE_TO 為必填'),
  ADMIN_TOKEN: z
    .string()
    .min(8, 'ADMIN_TOKEN 至少需要 8 個字元'),

  // 可選（有預設值）
  MIN_MARKET_CAP: z
    .string()
    .default('1000000000')
    .transform((v) => parseInt(v, 10)),
  MAX_SYMBOLS: z
    .string()
    .default('15')
    .transform((v) => parseInt(v, 10)),
  LOOKBACK_DAYS: z
    .string()
    .default('7')
    .transform((v) => parseInt(v, 10)),
  CONF_THRESHOLD: z
    .string()
    .default('0.65')
    .transform((v) => parseFloat(v)),
  REQUEST_DELAY_MS: z
    .string()
    .default('300')
    .transform((v) => parseInt(v, 10)),
  PORT: z
    .string()
    .default('3000')
    .transform((v) => parseInt(v, 10)),
  LOG_LEVEL: z
    .enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
    .default('info'),
});

/**
 * 驗證並解析環境變數
 */
function parseEnv() {
  const result = envSchema.safeParse(process.env);

  if (!result.success) {
    console.error('❌ 環境變數驗證失敗：');
    for (const issue of result.error.issues) {
      console.error(`   - ${issue.path.join('.')}: ${issue.message}`);
    }
    process.exit(1);
  }

  return result.data;
}

/**
 * 設定物件
 */
export const config = parseEnv();

/**
 * 設定型別
 */
export type Config = typeof config;

/**
 * 常數
 */
export const EASTERN_TIMEZONE = 'America/New_York';
export const DATE_FORMAT = 'yyyy-MM-dd';
export const CRON_SCHEDULE = '0 6 * * *'; // 每天 06:00
export const LINE_API_BASE = 'https://api.line.me/v2/bot';
export const LINE_MESSAGE_MAX_LENGTH = 3800;
export const LINE_MESSAGE_MAX_COUNT = 5;

/**
 * 輸出設定摘要（不含敏感資訊）
 */
export function logConfigSummary(): void {
  console.log('📋 設定摘要：');
  console.log(`   - ANALYSIS_API_BASE: ${config.ANALYSIS_API_BASE}`);
  console.log(`   - LINE_TO: ${config.LINE_TO.slice(0, 8)}...`);
  console.log(`   - MIN_MARKET_CAP: ${(config.MIN_MARKET_CAP / 1e9).toFixed(1)}B`);
  console.log(`   - MAX_SYMBOLS: ${config.MAX_SYMBOLS}`);
  console.log(`   - LOOKBACK_DAYS: ${config.LOOKBACK_DAYS}`);
  console.log(`   - CONF_THRESHOLD: ${(config.CONF_THRESHOLD * 100).toFixed(0)}%`);
  console.log(`   - REQUEST_DELAY_MS: ${config.REQUEST_DELAY_MS}ms`);
  console.log(`   - PORT: ${config.PORT}`);
  console.log(`   - LOG_LEVEL: ${config.LOG_LEVEL}`);
}
