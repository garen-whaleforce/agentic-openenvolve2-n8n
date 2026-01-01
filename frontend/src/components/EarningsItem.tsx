'use client';

import type { EarningsCallItem } from '@/lib/types';
import { formatMarketCap } from '@/lib/utils';
import { Badge } from './ui';

interface EarningsItemProps {
  item: EarningsCallItem;
  isSelected: boolean;
  isLoading: boolean;
  onClick: () => void;
}

export function EarningsItem({ item, isSelected, isLoading, onClick }: EarningsItemProps) {
  return (
    <button
      onClick={onClick}
      disabled={isLoading}
      className={`
        w-full text-left p-4 transition-all duration-150
        border-b border-gray-100 last:border-b-0
        hover:bg-gray-50 focus:outline-none focus:bg-gray-50
        disabled:opacity-50 disabled:cursor-not-allowed
        ${isSelected ? 'bg-primary-50 border-l-4 border-l-primary-500' : ''}
      `}
    >
      <div className="flex items-center justify-between">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3">
            <span className="font-bold text-gray-900 text-lg">{item.symbol}</span>
            <span className="text-gray-500 text-sm truncate">{item.company}</span>
          </div>
          <div className="flex items-center gap-2 mt-1.5">
            {item.sector && (
              <Badge variant="default" size="sm">
                {item.sector}
              </Badge>
            )}
            {item.exchange && (
              <span className="text-xs text-gray-400">{item.exchange}</span>
            )}
          </div>
        </div>
        <div className="text-right ml-4">
          <div className="text-sm font-medium text-gray-700">
            {formatMarketCap(item.market_cap)}
          </div>
          {item.eps_actual != null && item.eps_estimated != null && (
            <div className="text-xs text-gray-400 mt-1">
              EPS: {item.eps_actual.toFixed(2)} / {item.eps_estimated.toFixed(2)}
            </div>
          )}
        </div>
      </div>
      {isLoading && (
        <div className="mt-2 flex items-center gap-2 text-sm text-primary-600">
          <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
              fill="none"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
            />
          </svg>
          分析中...
        </div>
      )}
    </button>
  );
}
