import { NextRequest, NextResponse } from 'next/server';
import { DEFAULT_MIN_MARKET_CAP, API_TIMEOUT } from '@/lib/constants';

/**
 * BFF Proxy: GET /api/bff/earnings/today
 * 代理到後端 /api/earnings-calendar/today
 */
export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;

  const date = searchParams.get('date');
  const minMarketCap = searchParams.get('min_market_cap') || DEFAULT_MIN_MARKET_CAP.toString();

  // 參數驗證
  if (!date) {
    return NextResponse.json(
      { error: '缺少必要參數', message: 'date 為必填' },
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

  const analysisApiBase = process.env.ANALYSIS_API_BASE;
  if (!analysisApiBase) {
    return NextResponse.json(
      { error: '伺服器設定錯誤', message: 'ANALYSIS_API_BASE 未設定' },
      { status: 500 }
    );
  }

  try {
    const url = new URL(`${analysisApiBase}/api/earnings-calendar/today`);
    url.searchParams.set('date', date);
    url.searchParams.set('min_market_cap', minMarketCap);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT);

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[BFF] earnings/today error: ${response.status}`, errorText);
      return NextResponse.json(
        { error: '後端 API 錯誤', message: errorText.slice(0, 200) },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error('[BFF] earnings/today exception:', error);

    if (error instanceof Error && error.name === 'AbortError') {
      return NextResponse.json({ error: '請求逾時', message: '後端回應時間過長' }, { status: 504 });
    }

    return NextResponse.json(
      { error: '連線錯誤', message: error instanceof Error ? error.message : '未知錯誤' },
      { status: 500 }
    );
  }
}
