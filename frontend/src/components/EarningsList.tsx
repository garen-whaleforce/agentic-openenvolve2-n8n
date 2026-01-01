'use client';

import { useMemo, useState } from 'react';
import type { EarningsCallItem, GroupedEarningsCalls, SortOption, SelectedCall } from '@/lib/types';
import { groupCallsByDate, sortCalls, filterCalls, formatDateDisplay } from '@/lib/utils';
import { EarningsItem } from './EarningsItem';
import { Input, Select, DateGroupSkeleton } from './ui';

interface EarningsListProps {
  calls: EarningsCallItem[];
  isLoading: boolean;
  selectedCall: SelectedCall | null;
  loadingSymbol: string | null;
  onSelectCall: (call: SelectedCall) => void;
}

const sortOptions: { value: SortOption; label: string }[] = [
  { value: 'market_cap_desc', label: '市值 (高到低)' },
  { value: 'market_cap_asc', label: '市值 (低到高)' },
  { value: 'symbol_asc', label: 'Symbol (A-Z)' },
  { value: 'symbol_desc', label: 'Symbol (Z-A)' },
];

export function EarningsList({
  calls,
  isLoading,
  selectedCall,
  loadingSymbol,
  onSelectCall,
}: EarningsListProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState<SortOption>('market_cap_desc');

  // 處理資料：篩選 -> 排序 -> 分組
  const groupedCalls = useMemo<GroupedEarningsCalls[]>(() => {
    const filtered = filterCalls(calls, searchQuery);
    const sorted = sortCalls(filtered, sortBy);
    return groupCallsByDate(sorted);
  }, [calls, searchQuery, sortBy]);

  // 計算總數
  const totalCount = calls.length;
  const filteredCount = groupedCalls.reduce((sum, g) => sum + g.calls.length, 0);

  if (isLoading) {
    return (
      <div className="space-y-6">
        <div className="flex gap-4">
          <div className="flex-1 h-10 bg-gray-200 rounded-lg animate-pulse" />
          <div className="w-40 h-10 bg-gray-200 rounded-lg animate-pulse" />
        </div>
        <DateGroupSkeleton />
        <DateGroupSkeleton />
      </div>
    );
  }

  if (totalCount === 0) {
    return (
      <div className="text-center py-12">
        <div className="text-gray-400 text-5xl mb-4">📅</div>
        <h3 className="text-lg font-medium text-gray-700 mb-2">
          這三天沒有符合條件的 Earnings Call
        </h3>
        <p className="text-gray-500 text-sm">
          請嘗試調整日期或降低市值門檻
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* 搜尋與排序 */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1">
          <Input
            placeholder="搜尋 Symbol 或公司名稱..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            leftIcon={
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
            }
          />
        </div>
        <div className="w-full sm:w-44">
          <Select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as SortOption)}
            options={sortOptions}
          />
        </div>
      </div>

      {/* 計數 */}
      <div className="text-sm text-gray-500">
        {searchQuery ? (
          <>
            找到 {filteredCount} 筆（共 {totalCount} 筆）
          </>
        ) : (
          <>共 {totalCount} 筆</>
        )}
      </div>

      {/* 分組清單 */}
      {filteredCount === 0 ? (
        <div className="text-center py-8 text-gray-500">
          沒有符合「{searchQuery}」的結果
        </div>
      ) : (
        <div className="space-y-6">
          {groupedCalls.map((group) => (
            <div key={group.date}>
              <h3 className="text-sm font-semibold text-gray-600 mb-2 flex items-center gap-2">
                <span className="bg-gray-100 px-3 py-1 rounded-full">
                  {formatDateDisplay(group.date)}
                </span>
                <span className="text-gray-400 font-normal">
                  {group.calls.length} 家
                </span>
              </h3>
              <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
                {group.calls.map((item) => (
                  <EarningsItem
                    key={`${item.symbol}-${item.date}`}
                    item={item}
                    isSelected={
                      selectedCall?.symbol === item.symbol &&
                      selectedCall?.date === item.date
                    }
                    isLoading={
                      loadingSymbol === item.symbol &&
                      selectedCall?.date === item.date
                    }
                    onClick={() =>
                      onSelectCall({
                        symbol: item.symbol,
                        company: item.company,
                        date: item.date,
                      })
                    }
                  />
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
