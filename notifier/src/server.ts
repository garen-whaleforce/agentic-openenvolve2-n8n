/**
 * Express 伺服器
 */

import express, { Request, Response, NextFunction } from 'express';
import { config } from './config.js';
import logger from './logger.js';
import { sendTestMessage } from './line.js';
import { runDailyScan } from './runner.js';

const app = express();

// Middleware
app.use(express.json());

// Request logging
app.use((req: Request, _res: Response, next: NextFunction) => {
  logger.debug({ method: req.method, path: req.path }, 'Request');
  next();
});

/**
 * Health check
 */
app.get('/healthz', (_req: Request, res: Response) => {
  res.status(200).send('ok');
});

/**
 * Admin 認證 middleware
 */
function adminAuth(req: Request, res: Response, next: NextFunction): void {
  const token = req.headers['x-admin-token'];

  if (!token || token !== config.ADMIN_TOKEN) {
    logger.warn({ ip: req.ip }, '未授權的 admin 請求');
    res.status(401).json({ error: 'Unauthorized', message: '無效的 admin token' });
    return;
  }

  next();
}

/**
 * 測試 LINE 推播
 * POST /admin/test-line
 * Headers: x-admin-token: {ADMIN_TOKEN}
 * Body (optional): { "text": "自訂訊息" }
 */
app.post('/admin/test-line', adminAuth, async (req: Request, res: Response) => {
  try {
    const { text } = req.body as { text?: string };

    logger.info('執行 LINE 測試推播...');
    const result = await sendTestMessage(text);

    if (result.success) {
      logger.info('LINE 測試推播成功');
      res.status(200).json({
        success: true,
        message: 'LINE 推播成功',
        statusCode: result.statusCode,
      });
    } else {
      logger.error({ error: result.error }, 'LINE 測試推播失敗');
      res.status(500).json({
        success: false,
        message: 'LINE 推播失敗',
        error: result.error,
        statusCode: result.statusCode,
      });
    }
  } catch (error) {
    logger.error({ error }, 'LINE 測試推播異常');
    res.status(500).json({
      success: false,
      message: '內部錯誤',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

/**
 * 手動觸發每日掃描
 * POST /admin/run-scan
 * Headers: x-admin-token: {ADMIN_TOKEN}
 */
app.post('/admin/run-scan', adminAuth, async (_req: Request, res: Response) => {
  try {
    logger.info('手動觸發每日掃描...');

    // 非同步執行，立即回應
    runDailyScan().catch((error) => {
      logger.error({ error }, '手動掃描執行失敗');
    });

    res.status(202).json({
      success: true,
      message: '掃描已啟動，結果將透過 LINE 推播',
    });
  } catch (error) {
    logger.error({ error }, '手動掃描啟動失敗');
    res.status(500).json({
      success: false,
      message: '啟動失敗',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

/**
 * 取得服務狀態
 * GET /admin/status
 * Headers: x-admin-token: {ADMIN_TOKEN}
 */
app.get('/admin/status', adminAuth, (_req: Request, res: Response) => {
  const { DateTime } = require('luxon');
  const now = DateTime.now().setZone('America/New_York');

  res.status(200).json({
    service: 'earnings-call-notifier',
    status: 'running',
    etTime: now.toFormat('yyyy-MM-dd HH:mm:ss'),
    config: {
      analysisApiBase: config.ANALYSIS_API_BASE,
      lineToPrefix: config.LINE_TO.slice(0, 8) + '...',
      minMarketCap: config.MIN_MARKET_CAP,
      maxSymbols: config.MAX_SYMBOLS,
      lookbackDays: config.LOOKBACK_DAYS,
      confThreshold: config.CONF_THRESHOLD,
    },
  });
});

/**
 * 404 handler
 */
app.use((_req: Request, res: Response) => {
  res.status(404).json({ error: 'Not Found' });
});

/**
 * Error handler
 */
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error({ error: err.message }, 'Unhandled error');
  res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

/**
 * 啟動伺服器
 */
export function startServer(): void {
  app.listen(config.PORT, () => {
    logger.info({ port: config.PORT }, '🚀 伺服器啟動');
  });
}

export default app;
