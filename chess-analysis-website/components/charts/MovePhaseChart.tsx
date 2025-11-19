'use client';

import { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { GameData } from '@/lib/types';
import { groupByMoveRange } from '@/lib/utils';

interface MovePhaseChartProps {
  games: GameData[];
}

export function MovePhaseChart({ games }: MovePhaseChartProps) {
  const chartData = useMemo(() => {
    const phases = groupByMoveRange(games);

    return Object.entries(phases).map(([phase, moves]) => {
      const avgCPL = moves.length > 0
        ? moves.reduce((sum, m) => sum + m.centipawnLoss, 0) / moves.length
        : 0;

      return {
        phase,
        avgCPL: Math.round(avgCPL * 10) / 10,
        moves: moves.length,
      };
    });
  }, [games]);

  return (
    <div className="h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis dataKey="phase" stroke="#9CA3AF" />
          <YAxis stroke="#9CA3AF" label={{ value: 'Avg CPL', angle: -90, position: 'insideLeft', fill: '#9CA3AF' }} />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1F2937',
              border: '1px solid #374151',
              borderRadius: '8px',
              color: '#F3F4F6',
            }}
          />
          <Legend />
          <Bar dataKey="avgCPL" fill="#769656" name="Average CPL" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
