'use client';

import { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { GameData } from '@/lib/types';
import { getUniqueOpenings } from '@/lib/utils';

interface OpeningAnalysisProps {
  games: GameData[];
}

export function OpeningAnalysis({ games }: OpeningAnalysisProps) {
  const chartData = useMemo(() => {
    const openings = getUniqueOpenings(games);

    // Calculate average CPL for each opening
    const openingStats = openings.slice(0, 10).map(opening => {
      const openingGames = games.filter(g => g.openingEco === opening.eco);
      const allMoves = openingGames.flatMap(g => g.moves);
      const avgCPL = allMoves.length > 0
        ? allMoves.reduce((sum, m) => sum + m.centipawnLoss, 0) / allMoves.length
        : 0;

      return {
        name: `${opening.eco}: ${opening.name.substring(0, 20)}...`,
        avgCPL: Math.round(avgCPL * 10) / 10,
        games: opening.count,
      };
    });

    return openingStats.sort((a, b) => b.games - a.games);
  }, [games]);

  if (chartData.length === 0) {
    return (
      <div className="h-80 flex items-center justify-center text-gray-500">
        No opening data available
      </div>
    );
  }

  return (
    <div className="h-96">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={chartData}
          layout="vertical"
          margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis type="number" stroke="#9CA3AF" label={{ value: 'Average CPL', position: 'insideBottom', offset: -10, fill: '#9CA3AF' }} />
          <YAxis type="category" dataKey="name" stroke="#9CA3AF" width={150} />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1F2937',
              border: '1px solid #374151',
              borderRadius: '8px',
              color: '#F3F4F6',
            }}
          />
          <Bar dataKey="avgCPL" fill="#F0C040" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
