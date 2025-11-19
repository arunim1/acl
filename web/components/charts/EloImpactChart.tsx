'use client';

import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { AnalysisResult } from '@/lib/types';
import { formatNumber } from '@/lib/utils';

interface EloImpactChartProps {
  data: AnalysisResult['eloImpact'];
}

export function EloImpactChart({ data }: EloImpactChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="flex h-96 items-center justify-center text-gray-500">
        No ELO data available
      </div>
    );
  }

  const chartData = data.map((d) => ({
    eloRange: d.eloRange,
    avgLoss: parseFloat(d.avgLoss.toFixed(2)),
    count: d.count,
  }));

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart
        data={chartData}
        margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis
          dataKey="eloRange"
          label={{
            value: 'ELO Range',
            position: 'insideBottom',
            offset: -5,
          }}
          stroke="#6b7280"
          angle={-45}
          textAnchor="end"
          height={80}
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
        <Bar dataKey="avgLoss" fill="#8b5cf6" name="Average Loss" />
      </BarChart>
    </ResponsiveContainer>
  );
}
