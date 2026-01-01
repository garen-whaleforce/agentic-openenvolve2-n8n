import { NextRequest, NextResponse } from 'next/server';
import { API_TIMEOUT } from '@/lib/constants';
import type { AnalyzeRequest } from '@/lib/types';

/**
 * BFF Proxy: POST /api/bff/analyze
 * 代理到後端 POST /api/analyze
 */
export async function POST(request: NextRequest) {
  let body: AnalyzeRequest;

  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: '請求格式錯誤', message: '無效的 JSON body' },
      { status: 400 }
    );
  }

  const { symbol, date, refresh = false } = body;

  // 參數驗證
  if (!symbol || !date) {
    return NextResponse.json(
      { error: '缺少必要參數', message: 'symbol 和 date 為必填' },
      { status: 400 }
    );
  }

  // 驗證日期格式
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRegex.test(date)) {
    return NextResponse.json(
      { error: '日期格式錯誤', message: '日期格式應為 YYYY-MM-DD' },
      { status: 400 }
    );
  }

  // 驗證 symbol 格式（基本檢查）
  if (!/^[A-Z0-9.-]+$/i.test(symbol)) {
    return NextResponse.json(
      { error: 'Symbol 格式錯誤', message: 'Symbol 只能包含字母、數字、點和連字號' },
      { status: 400 }
    );
  }

  const analysisApiBase = process.env.ANALYSIS_API_BASE;
  if (!analysisApiBase) {
    return NextResponse.json(
      { error: '伺服器設定錯誤', message: 'ANALYSIS_API_BASE 未設定' },
      { status: 500 }
    );
  }

  try {
    const url = `${analysisApiBase}/api/analyze`;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT);

    console.log(`[BFF] analyze request: ${symbol} @ ${date}`);

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        symbol: symbol.toUpperCase(),
        date,
        refresh,
      }),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[BFF] analyze error: ${response.status}`, errorText);

      // 特殊處理 transcript 不存在的情況
      const lowerError = errorText.toLowerCase();
      if (
        lowerError.includes('transcript') ||
        lowerError.includes('not found') ||
        response.status === 404
      ) {
        return NextResponse.json(
          {
            error: 'TRANSCRIPT_PENDING',
            message: '尚未取得 transcript，請稍後重試',
            detail: errorText.slice(0, 200),
          },
          { status: 404 }
        );
      }

      return NextResponse.json(
        { error: '分析失敗', message: errorText.slice(0, 200) },
        { status: response.status }
      );
    }

    const data = await response.json();
    console.log(`[BFF] analyze success: ${symbol}`);
    return NextResponse.json(data);
  } catch (error) {
    console.error('[BFF] analyze exception:', error);

    if (error instanceof Error && error.name === 'AbortError') {
      return NextResponse.json(
        { error: '請求逾時', message: '分析時間過長，請稍後重試' },
        { status: 504 }
      );
    }

    return NextResponse.json(
      { error: '連線錯誤', message: error instanceof Error ? error.message : '未知錯誤' },
      { status: 500 }
    );
  }
}
