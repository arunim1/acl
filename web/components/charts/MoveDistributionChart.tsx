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
} from 'recharts';
import type { AnalysisResult } from '@/lib/types';
import { formatNumber } from '@/lib/utils';

interface MoveDistributionChartProps {
  data: AnalysisResult['moveDistribution'];
  maxMoves?: number;
}

export function MoveDistributionChart({
  data,
  maxMoves = 60,
}: MoveDistributionChartProps) {
  const chartData = data
    .filter((d) => d.moveNumber <= maxMoves)
    .map((d) => ({
      moveNumber: d.moveNumber,
      avgLoss: parseFloat(d.avgLoss.toFixed(2)),
      count: d.count,
    }));

  if (chartData.length === 0) {
    return (
      <div className="flex h-96 items-center justify-center text-gray-500">
        No data available
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart
        data={chartData}
        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis
          dataKey="moveNumber"
          label={{
            value: 'Move Number',
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
        />
        <Tooltip
          contentStyle={{
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            border: '1px solid #e5e7eb',
            borderRadius: '0.5rem',
          }}
          formatter={(value: number, name: string) => {
            if (name === 'count') return [value.toLocaleString(), 'Moves'];
            return [formatNumber(value), 'Avg Loss'];
          }}
        />
        <Legend />
        <Line
          type="monotone"
          dataKey="avgLoss"
          stroke="#10b981"
          strokeWidth={2}
          dot={{ fill: '#10b981', r: 3 }}
          name="Average Loss"
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
