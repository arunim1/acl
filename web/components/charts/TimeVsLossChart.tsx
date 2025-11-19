'use client';

import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  Area,
  ComposedChart,
} from 'recharts';
import type { AnalysisResult } from '@/lib/types';
import { formatNumber } from '@/lib/utils';

interface TimeVsLossChartProps {
  data: AnalysisResult['timeVsLossData'];
  maxTime?: number;
}

export function TimeVsLossChart({ data, maxTime = 60 }: TimeVsLossChartProps) {
  // Filter and prepare data
  const chartData = data
    .filter((d) => d.timeSpent <= maxTime && d.count >= 10)
    .map((d) => ({
      timeSpent: d.timeSpent,
      avgLoss: parseFloat(d.avgLoss.toFixed(2)),
      upperBound: parseFloat((d.avgLoss + d.stdDev).toFixed(2)),
      lowerBound: parseFloat(Math.max(0, d.avgLoss - d.stdDev).toFixed(2)),
      count: d.count,
    }));

  if (chartData.length === 0) {
    return (
      <div className="flex h-96 items-center justify-center text-gray-500">
        No data available for the selected filters
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={400}>
      <ComposedChart
        data={chartData}
        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis
          dataKey="timeSpent"
          label={{
            value: 'Time Spent (seconds)',
            position: 'insideBottom',
            offset: -5,
          }}
          stroke="#6b7280"
        />
        <YAxis
          label={{
            value: 'Average Centipawn Loss',
            angle: -90,
            position: 'insideLeft',
          }}
          stroke="#6b7280"
          domain={[0, 'auto']}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            border: '1px solid #e5e7eb',
            borderRadius: '0.5rem',
          }}
          formatter={(value: number, name: string) => {
            if (name === 'count') return [value.toLocaleString(), 'Moves'];
            return [formatNumber(value), name === 'avgLoss' ? 'Avg Loss' : name];
          }}
        />
        <Legend />

        {/* Standard deviation area */}
        <Area
          type="monotone"
          dataKey="upperBound"
          stackId="1"
          stroke="none"
          fill="#93c5fd"
          fillOpacity={0.3}
          name="±1 Std Dev"
        />
        <Area
          type="monotone"
          dataKey="lowerBound"
          stackId="1"
          stroke="none"
          fill="transparent"
        />

        {/* Mean line */}
        <Line
          type="monotone"
          dataKey="avgLoss"
          stroke="#2563eb"
          strokeWidth={2}
          dot={{ fill: '#2563eb', r: 3 }}
          name="Average Loss"
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
