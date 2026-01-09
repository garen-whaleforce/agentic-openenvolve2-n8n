/**
 * 排程模組
 */

import cron from 'node-cron';
import { DateTime } from 'luxon';
import { EASTERN_TIMEZONE } from './config.js';
import logger from './logger.js';
import { runDailyScan } from './runner.js';

let scheduledTask: cron.ScheduledTask | null = null;

// 主掃描排程：每天 18:00 ET（台灣時間隔天 07:00）
const MAIN_SCHEDULE = '0 18 * * *';

/**
 * 啟動排程
 */
export function startScheduler(): void {
  if (scheduledTask) {
    logger.warn('排程已在運行中');
    return;
  }

  logger.info(
    { schedule: MAIN_SCHEDULE, timezone: EASTERN_TIMEZONE },
    '⏰ 啟動排程'
  );

  scheduledTask = cron.schedule(
    MAIN_SCHEDULE,
    async () => {
      logger.info('⏰ 排程觸發：開始每日掃描（今天+昨天）');
      try {
        // 掃描今天和昨天的 transcripts（使用 range 模式，lookbackDays=2）
        await runDailyScan({ useRangeMode: true, lookbackDays: 2 });
        logger.info('⏰ 排程執行完成');
      } catch (error) {
        logger.error({ error }, '⏰ 排程執行失敗');
      }
    },
    {
      timezone: EASTERN_TIMEZONE,
      scheduled: true,
    }
  );

  logger.info('✅ 主掃描排程已啟動：每天 18:00 ET（台灣時間 07:00）執行');
}

/**
 * 停止排程
 */
export function stopScheduler(): void {
  if (scheduledTask) {
    scheduledTask.stop();
    scheduledTask = null;
    logger.info('⏹️ 主掃描排程已停止');
  }
}

/**
 * 取得下次執行時間
 */
export function getNextRunTime(): string {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);

  // 今天 18:00 ET
  let next = now.set({ hour: 18, minute: 0, second: 0, millisecond: 0 });

  // 如果已經過了今天 18:00，就是明天 18:00
  if (now >= next) {
    next = next.plus({ days: 1 });
  }

  return next.toFormat('yyyy-MM-dd HH:mm:ss');
}
