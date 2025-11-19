'use client';

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import type { AnalysisResult } from '@/lib/types';
import { formatNumber, formatLargeNumber } from '@/lib/utils';
import { BarChart3, TrendingDown, Clock, Database } from 'lucide-react';

interface StatsOverviewProps {
  analysisResult: AnalysisResult;
}

export function StatsOverview({ analysisResult }: StatsOverviewProps) {
  const stats = [
    {
      title: 'Total Games',
      value: formatLargeNumber(analysisResult.totalGames),
      icon: Database,
      color: 'text-blue-600',
      bgColor: 'bg-blue-100',
    },
    {
      title: 'Total Moves',
      value: formatLargeNumber(analysisResult.totalMoves),
      icon: BarChart3,
      color: 'text-green-600',
      bgColor: 'bg-green-100',
    },
    {
      title: 'Avg Centipawn Loss',
      value: formatNumber(analysisResult.avgCentipawnLoss),
      icon: TrendingDown,
      color: 'text-red-600',
      bgColor: 'bg-red-100',
    },
    {
      title: 'Avg Time Spent',
      value: `${formatNumber(analysisResult.avgTimeSpent)}s`,
      icon: Clock,
      color: 'text-purple-600',
      bgColor: 'bg-purple-100',
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {stats.map((stat) => {
        const Icon = stat.icon;
        return (
          <Card key={stat.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-gray-600">
                {stat.title}
              </CardTitle>
              <div className={`p-2 rounded-full ${stat.bgColor}`}>
                <Icon className={`h-4 w-4 ${stat.color}`} />
              </div>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{stat.value}</div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}
