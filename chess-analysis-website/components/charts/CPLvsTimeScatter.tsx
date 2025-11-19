'use client';

import { useMemo } from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ZAxis } from 'recharts';
import { GameData } from '@/lib/types';
import { gamesToChartData } from '@/lib/utils';

interface CPLvsTimeScatterProps {
  games: GameData[];
}

export function CPLvsTimeScatter({ games }: CPLvsTimeScatterProps) {
  const chartData = useMemo(() => {
    const points = gamesToChartData(games, 5000); // Limit to 5000 points for performance
    return points.filter(p => p.centipawnLoss <= 300 && p.timeSpent <= 120); // Filter outliers
  }, [games]);

  if (chartData.length === 0) {
    return (
      <div className="h-80 flex items-center justify-center text-gray-500">
        No data available with current filters
      </div>
    );
  }

  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            type="number"
            dataKey="timeSpent"
            name="Time Spent"
            unit="s"
            stroke="#9CA3AF"
            label={{ value: 'Time Spent (seconds)', position: 'insideBottom', offset: -10, fill: '#9CA3AF' }}
          />
          <YAxis
            type="number"
            dataKey="centipawnLoss"
            name="Centipawn Loss"
            stroke="#9CA3AF"
            label={{ value: 'Centipawn Loss', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }}
          />
          <ZAxis type="number" dataKey="elo" range={[20, 100]} />
          <Tooltip
            cursor={{ strokeDasharray: '3 3' }}
            contentStyle={{
              backgroundColor: '#1F2937',
              border: '1px solid #374151',
              borderRadius: '8px',
              color: '#F3F4F6',
            }}
            formatter={(value: any, name: string) => {
              if (name === 'Time Spent') return `${value}s`;
              if (name === 'Centipawn Loss') return `${Math.round(value)}`;
              return value;
            }}
          />
          <Scatter
            name="Moves"
            data={chartData}
            fill="#769656"
            fillOpacity={0.6}
          />
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}
