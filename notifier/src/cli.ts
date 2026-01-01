/**
 * CLI 工具
 */

import { logConfigSummary } from './config.js';
import logger from './logger.js';
import { sendTestMessage } from './line.js';
import { runDailyScan } from './runner.js';

/**
 * 取得命令
 */
function getCommand(): string {
  const args = process.argv.slice(2);
  return args[0] || '';
}

/**
 * 測試 LINE 推播
 */
async function testLine(): Promise<void> {
  console.log('');
  console.log('📱 測試 LINE 推播...');
  console.log('');

  const result = await sendTestMessage();

  if (result.success) {
    console.log('✅ LINE 推播成功！');
    console.log(`   Status: ${result.statusCode}`);
  } else {
    console.log('❌ LINE 推播失敗');
    console.log(`   Status: ${result.statusCode}`);
    console.log(`   Error: ${result.error}`);
    process.exit(1);
  }
}

/**
 * 執行一次掃描
 */
async function runOnce(): Promise<void> {
  console.log('');
  console.log('🔍 手動執行每日掃描...');
  console.log('');
  logConfigSummary();
  console.log('');

  const result = await runDailyScan();

  if (result) {
    console.log('');
    console.log('✅ 掃描完成！');
    console.log(`   目標日期：${result.targetDate}`);
    console.log(`   分析檔數：${result.analyzedCount}`);
    console.log(`   BUY：${result.buyCount}`);
    console.log(`   NO ACTION：${result.noActionCount}`);
    console.log(`   PENDING：${result.pendingCount}`);
    console.log(`   ERROR：${result.errorCount}`);
  } else {
    console.log('');
    console.log('⚠️ 掃描完成，但沒有結果');
  }
}

/**
 * 顯示使用說明
 */
function showHelp(): void {
  console.log('');
  console.log('Earnings Call Notifier CLI');
  console.log('');
  console.log('Usage:');
  console.log('  npm run test:line   - 發送 LINE 測試訊息');
  console.log('  npm run run:once    - 立即執行一次每日掃描');
  console.log('');
  console.log('或直接執行：');
  console.log('  tsx src/cli.ts test-line');
  console.log('  tsx src/cli.ts run-once');
  console.log('');
}

/**
 * 主函式
 */
async function main(): Promise<void> {
  const command = getCommand();

  switch (command) {
    case 'test-line':
      await testLine();
      break;

    case 'run-once':
      await runOnce();
      break;

    case 'help':
    case '--help':
    case '-h':
      showHelp();
      break;

    default:
      console.log(`❌ 未知命令：${command || '(空)'}`);
      showHelp();
      process.exit(1);
  }
}

main().catch((error) => {
  logger.fatal({ error }, 'CLI 執行失敗');
  process.exit(1);
});
