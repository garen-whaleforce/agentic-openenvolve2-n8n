/**
 * LINE Messaging API 模組
 */

import axios, { AxiosError } from 'axios';
import { config, LINE_API_BASE, LINE_MESSAGE_MAX_LENGTH, LINE_MESSAGE_MAX_COUNT } from './config.js';
import logger from './logger.js';
import type { LineTextMessage, LinePushRequest, LineApiResponse } from './types.js';

/**
 * LINE API Axios 實例
 */
const lineClient = axios.create({
  baseURL: LINE_API_BASE,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${config.LINE_CHANNEL_ACCESS_TOKEN}`,
  },
});

/**
 * 推送訊息到 LINE
 */
export async function pushMessage(messages: LineTextMessage[]): Promise<LineApiResponse> {
  if (messages.length === 0) {
    logger.warn('沒有訊息要推送');
    return { success: true };
  }

  // 限制最多 5 則訊息
  const limitedMessages = messages.slice(0, LINE_MESSAGE_MAX_COUNT);
  if (messages.length > LINE_MESSAGE_MAX_COUNT) {
    logger.warn(
      { original: messages.length, limited: LINE_MESSAGE_MAX_COUNT },
      '訊息數量超過上限，已截斷'
    );
  }

  const payload: LinePushRequest = {
    to: config.LINE_TO,
    messages: limitedMessages,
  };

  try {
    logger.info({ messageCount: limitedMessages.length }, '推送 LINE 訊息...');

    const response = await lineClient.post('/message/push', payload);

    logger.info({ status: response.status }, 'LINE 推送成功');
    return { success: true, statusCode: response.status };
  } catch (error) {
    const axiosError = error as AxiosError;
    const status = axiosError.response?.status;
    const errorData = axiosError.response?.data as { message?: string } | undefined;
    const errorMessage = errorData?.message || axiosError.message;

    logger.error(
      { status, error: errorMessage },
      'LINE 推送失敗'
    );

    return {
      success: false,
      statusCode: status,
      error: errorMessage,
    };
  }
}

/**
 * 推送單一文字訊息
 */
export async function pushTextMessage(text: string): Promise<LineApiResponse> {
  const chunks = chunkText(text);
  const messages: LineTextMessage[] = chunks.map((chunk) => ({
    type: 'text',
    text: chunk,
  }));

  return pushMessage(messages);
}

/**
 * 推送多段文字訊息
 */
export async function pushMultipleTexts(texts: string[]): Promise<LineApiResponse> {
  const messages: LineTextMessage[] = [];

  for (const text of texts) {
    const chunks = chunkText(text);
    for (const chunk of chunks) {
      messages.push({ type: 'text', text: chunk });
    }
  }

  return pushMessage(messages);
}

/**
 * 將長文字切分成多段
 */
export function chunkText(text: string): string[] {
  if (text.length <= LINE_MESSAGE_MAX_LENGTH) {
    return [text];
  }

  const chunks: string[] = [];
  let remaining = text;

  while (remaining.length > 0) {
    if (chunks.length >= LINE_MESSAGE_MAX_COUNT) {
      // 超過最大訊息數，截斷並加提示
      const lastChunk = chunks[chunks.length - 1];
      if (lastChunk && !lastChunk.endsWith('...（內容過長已截斷）')) {
        chunks[chunks.length - 1] = lastChunk.slice(0, -30) + '\n...（內容過長已截斷）';
      }
      break;
    }

    if (remaining.length <= LINE_MESSAGE_MAX_LENGTH) {
      chunks.push(remaining);
      break;
    }

    // 找到適當的斷點（換行符號）
    let breakPoint = remaining.lastIndexOf('\n', LINE_MESSAGE_MAX_LENGTH);
    if (breakPoint === -1 || breakPoint < LINE_MESSAGE_MAX_LENGTH * 0.5) {
      // 如果找不到好的斷點，就直接切
      breakPoint = LINE_MESSAGE_MAX_LENGTH;
    }

    chunks.push(remaining.slice(0, breakPoint));
    remaining = remaining.slice(breakPoint).trimStart();
  }

  return chunks;
}

/**
 * 發送測試訊息
 */
export async function sendTestMessage(customText?: string): Promise<LineApiResponse> {
  const { DateTime } = await import('luxon');
  const now = DateTime.now().setZone('America/New_York');
  const timestamp = now.toFormat('yyyy-MM-dd HH:mm:ss');

  const text =
    customText ||
    `🔔 Earnings Call Notifier 測試訊息\n\n` +
      `服務名稱：earnings-call-notifier\n` +
      `美東時間：${timestamp}\n` +
      `狀態：✅ 正常運作中`;

  return pushTextMessage(text);
}

/**
 * 格式化市值
 */
export function formatMarketCap(marketCap: number | null | undefined): string {
  if (marketCap == null) return 'N/A';

  if (marketCap >= 1e12) {
    return `$${(marketCap / 1e12).toFixed(1)}T`;
  }
  if (marketCap >= 1e9) {
    return `$${(marketCap / 1e9).toFixed(1)}B`;
  }
  if (marketCap >= 1e6) {
    return `$${(marketCap / 1e6).toFixed(1)}M`;
  }
  return `$${marketCap.toLocaleString()}`;
}

/**
 * 格式化信心度
 */
export function formatConfidence(confidence: number): string {
  return `${Math.round(confidence * 100)}%`;
}
