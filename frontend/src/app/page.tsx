'use client';

import { useState, useCallback, useRef, useEffect } from 'react';
import useSWR from 'swr';
import { DateTime } from 'luxon';
import { EarningsList, AnalysisResult, DatePicker } from '@/components';
import { fetchEarningsRange, analyzeWithCache, ApiError } from '@/lib/api';
import { getDateRangeET, getTodayET } from '@/lib/utils';
import { LOOKBACK_DAYS, EASTERN_TIMEZONE, DATE_FORMAT, DEFAULT_MIN_MARKET_CAP } from '@/lib/constants';
import type { EarningsCallItem, AnalysisResponse, SelectedCall } from '@/lib/types';

/**
 * SWR fetcher for earnings range
 */
const earningsFetcher = async ([, startDate, endDate]: [string, string, string]) => {
  return fetchEarningsRange({
    start_date: startDate,
    end_date: endDate,
    min_market_cap: DEFAULT_MIN_MARKET_CAP,
    refresh: false,
  });
};

export default function HomePage() {
  // 計算美東時區的今天
  const todayET = getTodayET();

  // 狀態
  const [selectedDate, setSelectedDate] = useState(todayET);
  const [selectedCall, setSelectedCall] = useState<SelectedCall | null>(null);
  const [analysisResult, setAnalysisResult] = useState<AnalysisResponse | null>(null);
  const [analysisError, setAnalysisError] = useState<string | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);

  // 用於手機版滾動到結果區
  const resultRef = useRef<HTMLDivElement>(null);

  // 計算日期範圍
  const { startDate, endDate } = getDateRangeET(selectedDate, LOOKBACK_DAYS);

  // 使用 SWR 取得 Earnings 清單
  const {
    data: earningsCalls = [],
    error: earningsError,
    isLoading: isLoadingEarnings,
    mutate: refetchEarnings,
  } = useSWR<EarningsCallItem[]>(
    ['earnings-range', startDate, endDate],
    earningsFetcher,
    {
      revalidateOnFocus: false,
      dedupingInterval: 60000, // 1 分鐘內不重複請求
    }
  );

  // 處理日期變更
  const handleDateChange = useCallback((date: string) => {
    setSelectedDate(date);
    setSelectedCall(null);
    setAnalysisResult(null);
    setAnalysisError(null);
  }, []);

  // 處理選擇 Earnings Call
  const handleSelectCall = useCallback(async (call: SelectedCall) => {
    // 如果已經在分析中，不允許再次點擊
    if (isAnalyzing) return;

    // 如果點擊同一個，清除選擇
    if (selectedCall?.symbol === call.symbol && selectedCall?.date === call.date) {
      setSelectedCall(null);
      setAnalysisResult(null);
      setAnalysisError(null);
      return;
    }

    setSelectedCall(call);
    setAnalysisResult(null);
    setAnalysisError(null);
    setIsAnalyzing(true);

    try {
      const result = await analyzeWithCache(call.symbol, call.date);
      setAnalysisResult(result);
      setAnalysisError(null);

      // 手機版滾動到結果區
      if (window.innerWidth < 1024 && resultRef.current) {
        setTimeout(() => {
          resultRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 100);
      }
    } catch (err) {
      console.error('Analysis error:', err);
      if (err instanceof ApiError) {
        setAnalysisError(err.message);
      } else {
        setAnalysisError(err instanceof Error ? err.message : '未知錯誤');
      }
      setAnalysisResult(null);
    } finally {
      setIsAnalyzing(false);
    }
  }, [isAnalyzing, selectedCall]);

  // 重試分析
  const handleRetry = useCallback(async () => {
    if (!selectedCall) return;

    setAnalysisError(null);
    setIsAnalyzing(true);

    try {
      const result = await analyzeWithCache(selectedCall.symbol, selectedCall.date, true);
      setAnalysisResult(result);
      setAnalysisError(null);
    } catch (err) {
      console.error('Retry error:', err);
      if (err instanceof ApiError) {
        setAnalysisError(err.message);
      } else {
        setAnalysisError(err instanceof Error ? err.message : '未知錯誤');
      }
    } finally {
      setIsAnalyzing(false);
    }
  }, [selectedCall]);

  // 顯示當前美東時間
  const [currentETTime, setCurrentETTime] = useState('');
  useEffect(() => {
    const updateTime = () => {
      const now = DateTime.now().setZone(EASTERN_TIMEZONE);
      setCurrentETTime(now.toFormat('yyyy-MM-dd HH:mm'));
    };
    updateTime();
    const interval = setInterval(updateTime, 60000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      {/* 頂部控制列 */}
      <div className="mb-6 flex flex-col sm:flex-row sm:items-end gap-4">
        <DatePicker
          value={selectedDate}
          onChange={handleDateChange}
          label="選擇日期"
        />
        <div className="text-sm text-gray-500">
          <span className="block sm:inline">
            顯示 {startDate} ~ {endDate} 的 Earnings Calls
          </span>
          <span className="block sm:inline sm:ml-2 text-xs text-gray-400">
            (美東時間：{currentETTime})
          </span>
        </div>
      </div>

      {/* 錯誤提示 */}
      {earningsError && (
        <div className="mb-6 p-4 bg-danger-50 border border-danger-200 rounded-lg">
          <div className="flex items-center gap-2">
            <span className="text-danger-500">⚠️</span>
            <span className="text-danger-700 text-sm">
              載入 Earnings 清單失敗：
              {earningsError instanceof Error ? earningsError.message : '未知錯誤'}
            </span>
          </div>
          <button
            onClick={() => refetchEarnings()}
            className="mt-2 text-sm text-danger-600 hover:text-danger-700 underline"
          >
            重新載入
          </button>
        </div>
      )}

      {/* 主要內容區 - 桌機左右分欄，手機上下排列 */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        {/* 左側：Earnings 清單 */}
        <div className="lg:col-span-2">
          <div className="sticky top-20">
            <h2 className="text-lg font-semibold text-gray-800 mb-4">
              Earnings Calls
            </h2>
            <EarningsList
              calls={earningsCalls}
              isLoading={isLoadingEarnings}
              selectedCall={selectedCall}
              loadingSymbol={isAnalyzing ? selectedCall?.symbol ?? null : null}
              onSelectCall={handleSelectCall}
            />
          </div>
        </div>

        {/* 右側：分析結果 */}
        <div className="lg:col-span-3" ref={resultRef}>
          <h2 className="text-lg font-semibold text-gray-800 mb-4">
            分析結果
          </h2>
          <div className="sticky top-20">
            <AnalysisResult
              selectedCall={selectedCall}
              result={analysisResult}
              isLoading={isAnalyzing}
              error={analysisError}
              onRetry={handleRetry}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
