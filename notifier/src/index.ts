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

  // 啟動伺服器
  startServer();

  // 啟動排程
  startScheduler();

  // 顯示下次執行時間
  const nextRun = getNextRunTime();
  logger.info({ nextRun }, '📅 下次執行時間');

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
  logger.fatal({ error }, '啟動失敗');
  process.exit(1);
});
