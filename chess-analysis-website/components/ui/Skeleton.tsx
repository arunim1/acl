import { cn } from '@/lib/utils';
import { CSSProperties } from 'react';

interface SkeletonProps {
  className?: string;
  variant?: 'text' | 'circular' | 'rectangular';
  style?: CSSProperties;
}

export function Skeleton({ className, variant = 'rectangular', style }: SkeletonProps) {
  const variants = {
    text: 'h-4 rounded',
    circular: 'rounded-full',
    rectangular: 'rounded-lg',
  };

  return (
    <div
      className={cn(
        'animate-pulse bg-gray-700/50',
        variants[variant],
        className
      )}
      style={style}
    />
  );
}

export function ChartSkeleton() {
  return (
    <div className="h-80 flex flex-col gap-4 p-4">
      <Skeleton className="h-6 w-48" variant="text" />
      <div className="flex-1 flex items-end justify-between gap-2">
        {Array.from({ length: 12 }).map((_, i) => (
          <Skeleton
            key={i}
            className="flex-1"
            style={{ height: `${Math.random() * 60 + 40}%` }}
          />
        ))}
      </div>
      <Skeleton className="h-4 w-full" variant="text" />
    </div>
  );
}

export function StatCardSkeleton() {
  return (
    <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-6 space-y-3">
      <Skeleton className="h-6 w-6" variant="circular" />
      <Skeleton className="h-4 w-24" variant="text" />
      <Skeleton className="h-8 w-32" variant="text" />
      <Skeleton className="h-3 w-20" variant="text" />
    </div>
  );
}
