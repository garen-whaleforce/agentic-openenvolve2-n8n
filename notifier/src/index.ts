/**
 * 應用程式入口
 */

import { logConfigSummary } from './config.js';
import logger from './logger.js';
import { startServer } from './server.js';
import { startScheduler, getNextRunTime } from './cron.js';

/**
 * 主函式
 */
async function main(): Promise<void> {
  console.log('');
  console.log('╔════════════════════════════════════════╗');
  console.log('║   Earnings Call Notifier v1.0.0        ║');
  console.log('║   Daily Analysis + LINE Push           ║');
  console.log('╚════════════════════════════════════════╝');
  console.log('');

  // 輸出設定摘要
  logConfigSummary();
  console.log('');

  // 啟動排程
  startScheduler();

  // 顯示下次執行時間
  try {
    const nextRun = getNextRunTime();
    logger.info({ nextRun }, '📅 下次執行時間');
  } catch (error) {
    logger.warn({ error: String(error) }, '無法計算下次執行時間');
  }

  // 啟動伺服器
  await startServer();
  logger.info('所有服務啟動完成，等待請求...');

  // 優雅關閉
  process.on('SIGTERM', () => {
    logger.info('收到 SIGTERM，準備關閉...');
    process.exit(0);
  });

  process.on('SIGINT', () => {
    logger.info('收到 SIGINT，準備關閉...');
    process.exit(0);
  });
}

main().catch((error) => {
  const errorMessage = error instanceof Error ? error.message : String(error);
  const errorStack = error instanceof Error ? error.stack : undefined;
  logger.fatal({ error: errorMessage, stack: errorStack }, '啟動失敗');
  console.error('啟動失敗:', errorMessage);
  if (errorStack) console.error(errorStack);
  process.exit(1);
});
