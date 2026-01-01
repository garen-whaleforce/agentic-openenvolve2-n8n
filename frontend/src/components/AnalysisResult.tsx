'use client';

import { useState } from 'react';
import type { AnalysisResponse, SelectedCall } from '@/lib/types';
import {
  formatConfidence,
  getPredictionDisplay,
  getTradeRecommendation,
  isTranscriptPendingError,
} from '@/lib/utils';
import { MAX_REASONS_TO_SHOW } from '@/lib/constants';
import { Card, CardContent, CardFooter, Button, Badge, AnalysisResultSkeleton } from './ui';

interface AnalysisResultProps {
  selectedCall: SelectedCall | null;
  result: AnalysisResponse | null;
  isLoading: boolean;
  error: string | null;
  onRetry: () => void;
}

export function AnalysisResult({
  selectedCall,
  result,
  isLoading,
  error,
  onRetry,
}: AnalysisResultProps) {
  const [showAllReasons, setShowAllReasons] = useState(false);
  const [showSummary, setShowSummary] = useState(false);

  // 未選擇狀態
  if (!selectedCall) {
    return (
      <Card className="h-full">
        <CardContent className="h-full flex flex-col items-center justify-center py-16 text-center">
          <div className="text-gray-300 text-6xl mb-4">📊</div>
          <h3 className="text-lg font-medium text-gray-600 mb-2">
            選擇一家公司進行分析
          </h3>
          <p className="text-gray-400 text-sm max-w-xs">
            從左側清單中點選任一 Earnings Call，系統將自動進行 AI 分析
          </p>
        </CardContent>
      </Card>
    );
  }

  // 載入中
  if (isLoading) {
    return (
      <Card>
        <div className="px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-3">
            <span className="font-bold text-xl text-gray-900">{selectedCall.symbol}</span>
            <Badge variant="info" size="sm">分析中</Badge>
          </div>
          <p className="text-gray-500 text-sm mt-1">{selectedCall.company}</p>
        </div>
        <AnalysisResultSkeleton />
      </Card>
    );
  }

  // 錯誤狀態
  if (error) {
    const isPending = isTranscriptPendingError(error);

    return (
      <Card>
        <CardContent className="py-8 text-center">
          <div className="flex items-center justify-center gap-3 mb-4">
            <span className="font-bold text-xl text-gray-900">{selectedCall.symbol}</span>
            <span className="text-gray-500">{selectedCall.company}</span>
          </div>

          {isPending ? (
            <>
              <div className="text-warning-500 text-5xl mb-4">⏳</div>
              <h3 className="text-lg font-medium text-gray-700 mb-2">
                PENDING：尚未取得 Transcript
              </h3>
              <p className="text-gray-500 text-sm mb-6 max-w-sm mx-auto">
                財報電話會議紀錄可能尚未公開，請稍後再試
              </p>
            </>
          ) : (
            <>
              <div className="text-danger-500 text-5xl mb-4">⚠️</div>
              <h3 className="text-lg font-medium text-gray-700 mb-2">分析失敗</h3>
              <p className="text-gray-500 text-sm mb-6 max-w-sm mx-auto break-words">
                {error.length > 150 ? error.slice(0, 150) + '...' : error}
              </p>
            </>
          )}

          <Button onClick={onRetry} variant="secondary">
            重試
          </Button>
        </CardContent>
      </Card>
    );
  }

  // 成功顯示結果
  if (!result) {
    return null;
  }

  const { agentic_result: analysis } = result;
  const prediction = getPredictionDisplay(analysis.prediction);
  const recommendation = getTradeRecommendation(analysis.trade_long);
  const reasons = analysis.reasons || [];
  const displayReasons = showAllReasons ? reasons : reasons.slice(0, MAX_REASONS_TO_SHOW);
  const directionScore = analysis.long_eligible_json?.DirectionScore;

  return (
    <Card>
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center gap-3">
              <span className="font-bold text-2xl text-gray-900">{result.symbol}</span>
              {result.transcript_date && (
                <Badge variant="default" size="sm">
                  {result.transcript_date}
                </Badge>
              )}
            </div>
            <p className="text-gray-500 text-sm mt-1">{selectedCall.company}</p>
          </div>
          {/* 建議 Badge */}
          <div
            className={`
              px-4 py-2 rounded-lg border-2
              ${recommendation.bgClass} ${recommendation.borderClass}
            `}
          >
            <span className={`font-bold text-lg ${recommendation.colorClass}`}>
              {recommendation.text}
            </span>
          </div>
        </div>
      </div>

      <CardContent className="space-y-6">
        {/* 指標區 */}
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
          {/* T+30 趨勢 */}
          <div className={`p-4 rounded-lg ${prediction.bgClass}`}>
            <div className="text-sm text-gray-600 mb-1">T+30 趨勢</div>
            <div className={`text-xl font-bold ${prediction.colorClass}`}>
              {prediction.text}
            </div>
          </div>

          {/* 信心度 */}
          <div className="p-4 rounded-lg bg-gray-50">
            <div className="text-sm text-gray-600 mb-1">T+30 信心</div>
            <div className="text-xl font-bold text-gray-900">
              {formatConfidence(analysis.confidence)}
            </div>
          </div>

          {/* Direction Score */}
          {directionScore != null && (
            <div className="p-4 rounded-lg bg-primary-50 col-span-2 lg:col-span-1">
              <div className="text-sm text-gray-600 mb-1">Direction Score</div>
              <div className="text-xl font-bold text-primary-700">
                {directionScore} / 10
              </div>
            </div>
          )}
        </div>

        {/* 分析理由 */}
        <div>
          <h4 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
            分析理由
            <Badge variant="default" size="sm">{reasons.length} 項</Badge>
          </h4>
          {reasons.length > 0 ? (
            <ul className="space-y-2">
              {displayReasons.map((reason, idx) => (
                <li
                  key={idx}
                  className="flex items-start gap-2 text-sm text-gray-600"
                >
                  <span className="text-primary-500 mt-0.5">•</span>
                  <span>{reason}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-gray-400">無分析理由</p>
          )}
          {reasons.length > MAX_REASONS_TO_SHOW && (
            <button
              onClick={() => setShowAllReasons(!showAllReasons)}
              className="mt-2 text-sm text-primary-600 hover:text-primary-700 font-medium"
            >
              {showAllReasons ? '收起' : `展開更多 (${reasons.length - MAX_REASONS_TO_SHOW} 項)`}
            </button>
          )}
        </div>

        {/* 摘要 */}
        {analysis.summary && (
          <div>
            <h4 className="text-sm font-semibold text-gray-700 mb-2 flex items-center justify-between">
              <span>分析摘要</span>
              <button
                onClick={() => setShowSummary(!showSummary)}
                className="text-xs text-primary-600 hover:text-primary-700 font-medium"
              >
                {showSummary ? '收起' : '展開'}
              </button>
            </h4>
            {showSummary && (
              <div className="p-4 bg-gray-50 rounded-lg text-sm text-gray-600 leading-relaxed">
                {analysis.summary}
              </div>
            )}
          </div>
        )}
      </CardContent>

      {/* 風險提示 */}
      <CardFooter>
        <p className="text-xs text-gray-400 text-center">
          ⚠️ 本分析結果僅供參考，非投資建議，請自行評估風險
        </p>
      </CardFooter>
    </Card>
  );
}
