'use client';

import { HTMLAttributes, forwardRef } from 'react';

interface SkeletonProps extends HTMLAttributes<HTMLDivElement> {
  width?: string | number;
  height?: string | number;
  rounded?: 'sm' | 'md' | 'lg' | 'full';
}

const roundedStyles = {
  sm: 'rounded',
  md: 'rounded-md',
  lg: 'rounded-lg',
  full: 'rounded-full',
};

export const Skeleton = forwardRef<HTMLDivElement, SkeletonProps>(
  ({ width, height, rounded = 'md', className = '', style, ...props }, ref) => {
    return (
      <div
        ref={ref}
        className={`
          bg-gray-200 animate-pulse
          ${roundedStyles[rounded]}
          ${className}
        `}
        style={{
          width: width,
          height: height,
          ...style,
        }}
        {...props}
      />
    );
  }
);

Skeleton.displayName = 'Skeleton';

/**
 * Earnings Call 項目的 Skeleton
 */
export function EarningsItemSkeleton() {
  return (
    <div className="flex items-center justify-between p-4 border-b border-gray-100 last:border-b-0">
      <div className="flex-1">
        <div className="flex items-center gap-3">
          <Skeleton width={60} height={24} />
          <Skeleton width={150} height={16} />
        </div>
        <Skeleton width={80} height={14} className="mt-2" />
      </div>
      <Skeleton width={70} height={20} />
    </div>
  );
}

/**
 * 分析結果的 Skeleton
 */
export function AnalysisResultSkeleton() {
  return (
    <div className="space-y-6 p-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="space-y-2">
          <Skeleton width={120} height={28} />
          <Skeleton width={200} height={16} />
        </div>
        <Skeleton width={100} height={40} rounded="lg" />
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-2 gap-4">
        <div className="p-4 bg-gray-50 rounded-lg">
          <Skeleton width={80} height={14} />
          <Skeleton width={60} height={24} className="mt-2" />
        </div>
        <div className="p-4 bg-gray-50 rounded-lg">
          <Skeleton width={80} height={14} />
          <Skeleton width={60} height={24} className="mt-2" />
        </div>
      </div>

      {/* Reasons */}
      <div className="space-y-3">
        <Skeleton width={60} height={16} />
        <Skeleton height={16} />
        <Skeleton height={16} />
        <Skeleton height={16} width="80%" />
      </div>

      {/* Summary */}
      <div className="space-y-3">
        <Skeleton width={60} height={16} />
        <Skeleton height={60} />
      </div>
    </div>
  );
}

/**
 * 日期區塊的 Skeleton
 */
export function DateGroupSkeleton() {
  return (
    <div className="mb-6">
      <Skeleton width={140} height={24} className="mb-3" />
      <div className="bg-white rounded-xl border border-gray-200">
        <EarningsItemSkeleton />
        <EarningsItemSkeleton />
        <EarningsItemSkeleton />
      </div>
    </div>
  );
}
