/**
 * 排程模組
 */

import cron from 'node-cron';
import { CRON_SCHEDULE, EASTERN_TIMEZONE } from './config.js';
import logger from './logger.js';
import { runDailyScan } from './runner.js';

let scheduledTask: cron.ScheduledTask | null = null;

/**
 * 啟動排程
 */
export function startScheduler(): void {
  if (scheduledTask) {
    logger.warn('排程已在運行中');
    return;
  }

  logger.info(
    { schedule: CRON_SCHEDULE, timezone: EASTERN_TIMEZONE },
    '⏰ 啟動排程'
  );

  scheduledTask = cron.schedule(
    CRON_SCHEDULE,
    async () => {
      logger.info('⏰ 排程觸發：開始每日掃描');
      try {
        await runDailyScan();
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

  logger.info('✅ 排程已啟動：每天 06:00 ET 執行');
}

/**
 * 停止排程
 */
export function stopScheduler(): void {
  if (scheduledTask) {
    scheduledTask.stop();
    scheduledTask = null;
    logger.info('⏹️ 排程已停止');
  }
}

/**
 * 取得下次執行時間
 */
export function getNextRunTime(): string {
  const { DateTime } = require('luxon');
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);

  // 今天 06:00
  let next = now.set({ hour: 6, minute: 0, second: 0, millisecond: 0 });

  // 如果已經過了今天 06:00，就是明天 06:00
  if (now >= next) {
    next = next.plus({ days: 1 });
  }

  return next.toFormat('yyyy-MM-dd HH:mm:ss');
}
