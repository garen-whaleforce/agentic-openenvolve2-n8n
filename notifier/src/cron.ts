/**
 * 排程模組
 */

import cron from 'node-cron';
import { DateTime } from 'luxon';
import { CRON_SCHEDULE, EASTERN_TIMEZONE } from './config.js';
import logger from './logger.js';
import { runDailyScan, runRetryQueue } from './runner.js';

let scheduledTask: cron.ScheduledTask | null = null;
let retryTask: cron.ScheduledTask | null = null;

// 重試排程：每天 10:00, 14:00, 18:00 ET
const RETRY_SCHEDULE = '0 10,14,18 * * *';

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

  logger.info('✅ 主掃描排程已啟動：每天 06:00 ET 執行');

  // 啟動重試排程
  if (retryTask) {
    logger.warn('重試排程已在運行中');
  } else {
    logger.info(
      { schedule: RETRY_SCHEDULE, timezone: EASTERN_TIMEZONE },
      '⏰ 啟動重試排程'
    );

    retryTask = cron.schedule(
      RETRY_SCHEDULE,
      async () => {
        logger.info('⏰ 重試排程觸發：檢查待分析佇列');
        try {
          await runRetryQueue();
          logger.info('⏰ 重試排程執行完成');
        } catch (error) {
          logger.error({ error }, '⏰ 重試排程執行失敗');
        }
      },
      {
        timezone: EASTERN_TIMEZONE,
        scheduled: true,
      }
    );

    logger.info('✅ 重試排程已啟動：每天 10:00, 14:00, 18:00 ET 執行');
  }
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
  if (retryTask) {
    retryTask.stop();
    retryTask = null;
    logger.info('⏹️ 重試排程已停止');
  }
}

/**
 * 取得下次執行時間
 */
export function getNextRunTime(): string {
  const now = DateTime.now().setZone(EASTERN_TIMEZONE);

  // 今天 06:00
  let next = now.set({ hour: 6, minute: 0, second: 0, millisecond: 0 });

  // 如果已經過了今天 06:00，就是明天 06:00
  if (now >= next) {
    next = next.plus({ days: 1 });
  }

  return next.toFormat('yyyy-MM-dd HH:mm:ss');
}
