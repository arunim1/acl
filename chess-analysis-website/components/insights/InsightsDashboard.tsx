'use client';

import { AggregatedStats, ProcessingSummary } from '@/lib/types';
import { TrendingDown, Target, Clock, AlertTriangle } from 'lucide-react';

interface InsightsDashboardProps {
  stats: AggregatedStats;
  summary: ProcessingSummary;
}

export function InsightsDashboard({ stats, summary }: InsightsDashboardProps) {
  return (
    <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
      <InsightCard
        icon={<TrendingDown className="w-6 h-6" />}
        title="Avg Centipawn Loss"
        value={stats.averageCPL.toFixed(1)}
        subtitle={`Median: ${stats.medianCPL.toFixed(1)}`}
        color="text-chess-gold"
      />
      <InsightCard
        icon={<Target className="w-6 h-6" />}
        title="Accuracy Rate"
        value={`${stats.accuracyRate.toFixed(1)}%`}
        subtitle="Moves with CPL < 50"
        color="text-green-500"
      />
      <InsightCard
        icon={<AlertTriangle className="w-6 h-6" />}
        title="Blunder Rate"
        value={`${stats.blunderRate.toFixed(1)}%`}
        subtitle="Moves with CPL > 200"
        color="text-red-500"
      />
      <InsightCard
        icon={<Clock className="w-6 h-6" />}
        title="Avg Time/Move"
        value={`${stats.averageTimeSpent.toFixed(1)}s`}
        subtitle={`${stats.totalMoves.toLocaleString()} moves`}
        color="text-blue-500"
      />
    </div>
  );
}

function InsightCard({
  icon,
  title,
  value,
  subtitle,
  color,
}: {
  icon: React.ReactNode;
  title: string;
  value: string;
  subtitle: string;
  color: string;
}) {
  return (
    <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-6 hover:border-chess-green/30 transition-all">
      <div className="flex items-start justify-between mb-3">
        <div className={`${color}`}>{icon}</div>
      </div>
      <div className="text-sm text-gray-400 mb-1">{title}</div>
      <div className="text-3xl font-bold text-white mb-1">{value}</div>
      <div className="text-xs text-gray-500">{subtitle}</div>
    </div>
  );
}
