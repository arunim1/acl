'use client';

import { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { GameData } from '@/lib/types';

interface CPLDistributionProps {
  games: GameData[];
}

export function CPLDistribution({ games }: CPLDistributionProps) {
  const chartData = useMemo(() => {
    const allMoves = games.flatMap(game => game.moves);

    // Create bins for centipawn loss
    const bins = [
      { range: '0-10', min: 0, max: 10, count: 0, label: 'Excellent' },
      { range: '10-25', min: 10, max: 25, count: 0, label: 'Good' },
      { range: '25-50', min: 25, max: 50, count: 0, label: 'Inaccuracy' },
      { range: '50-100', min: 50, max: 100, count: 0, label: 'Mistake' },
      { range: '100-200', min: 100, max: 200, count: 0, label: 'Blunder' },
      { range: '200+', min: 200, max: Infinity, count: 0, label: 'Huge Blunder' },
    ];

    for (const move of allMoves) {
      const cpl = move.centipawnLoss;
      const bin = bins.find(b => cpl >= b.min && cpl < b.max);
      if (bin) bin.count++;
    }

    return bins.map(bin => ({
      range: bin.range,
      count: bin.count,
      percentage: allMoves.length > 0 ? (bin.count / allMoves.length) * 100 : 0,
      label: bin.label,
    }));
  }, [games]);

  if (chartData.every(d => d.count === 0)) {
    return (
      <div className="h-80 flex items-center justify-center text-gray-500">
        No data available with current filters
      </div>
    );
  }

  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="range"
            stroke="#9CA3AF"
            label={{ value: 'Centipawn Loss Range', position: 'insideBottom', offset: -10, fill: '#9CA3AF' }}
          />
          <YAxis
            stroke="#9CA3AF"
            label={{ value: 'Number of Moves', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1F2937',
              border: '1px solid #374151',
              borderRadius: '8px',
              color: '#F3F4F6',
            }}
            formatter={(value: any, name: string, props: any) => {
              if (name === 'count') {
                return [
                  `${value.toLocaleString()} moves (${props.payload.percentage.toFixed(1)}%)`,
                  props.payload.label
                ];
              }
              return value;
            }}
          />
          <Bar dataKey="count" fill="#769656" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
