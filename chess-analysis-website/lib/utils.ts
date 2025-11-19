import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"
import { GameData, Move, AggregatedStats, FilterOptions, ChartDataPoint } from './types';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Calculate aggregated statistics from game data
 */
export function calculateStats(games: GameData[], filters?: FilterOptions): AggregatedStats {
  const filteredGames = filters ? filterGames(games, filters) : games;
  const allMoves: Move[] = filteredGames.flatMap(game => game.moves);

  if (allMoves.length === 0) {
    return {
      averageCPL: 0,
      medianCPL: 0,
      blunderRate: 0,
      accuracyRate: 0,
      averageTimeSpent: 0,
      totalMoves: 0,
      gamesAnalyzed: 0,
    };
  }

  const cpls = allMoves.map(m => m.centipawnLoss).sort((a, b) => a - b);
  const timeSpents = allMoves.map(m => m.timeSpent);

  const averageCPL = cpls.reduce((sum, cpl) => sum + cpl, 0) / cpls.length;
  const medianCPL = cpls[Math.floor(cpls.length / 2)];
  const blunderRate = (allMoves.filter(m => m.centipawnLoss > 200).length / allMoves.length) * 100;
  const accuracyRate = (allMoves.filter(m => m.centipawnLoss < 50).length / allMoves.length) * 100;
  const averageTimeSpent = timeSpents.reduce((sum, t) => sum + t, 0) / timeSpents.length;

  return {
    averageCPL: Math.round(averageCPL * 10) / 10,
    medianCPL: Math.round(medianCPL * 10) / 10,
    blunderRate: Math.round(blunderRate * 10) / 10,
    accuracyRate: Math.round(accuracyRate * 10) / 10,
    averageTimeSpent: Math.round(averageTimeSpent * 10) / 10,
    totalMoves: allMoves.length,
    gamesAnalyzed: filteredGames.length,
  };
}

/**
 * Filter games based on criteria
 */
export function filterGames(games: GameData[], filters: FilterOptions): GameData[] {
  return games.filter(game => {
    // ELO filtering
    if (filters.eloMin && (game.whiteElo < filters.eloMin || game.blackElo < filters.eloMin)) {
      return false;
    }
    if (filters.eloMax && (game.whiteElo > filters.eloMax || game.blackElo > filters.eloMax)) {
      return false;
    }

    // Time control filtering
    if (filters.timeControls && filters.timeControls.length > 0) {
      if (!filters.timeControls.includes(game.timeControl)) {
        return false;
      }
    }

    // Opening filtering
    if (filters.openings && filters.openings.length > 0) {
      if (!filters.openings.includes(game.openingEco)) {
        return false;
      }
    }

    // Result filtering
    if (filters.result && filters.result.length > 0) {
      if (!filters.result.includes(game.result)) {
        return false;
      }
    }

    // Date range filtering
    if (filters.dateRange) {
      if (game.date < filters.dateRange.start || game.date > filters.dateRange.end) {
        return false;
      }
    }

    return true;
  });
}

/**
 * Filter moves by color
 */
export function filterMovesByColor(games: GameData[], color?: 'white' | 'black'): GameData[] {
  if (!color) return games;

  return games.map(game => ({
    ...game,
    moves: game.moves.filter(move => move.color === color)
  }));
}

/**
 * Convert games to chart data points
 */
export function gamesToChartData(games: GameData[], limit?: number): ChartDataPoint[] {
  const points: ChartDataPoint[] = [];

  for (const game of games) {
    for (const move of game.moves) {
      points.push({
        timeSpent: move.timeSpent,
        centipawnLoss: move.centipawnLoss,
        moveNumber: move.moveNumber,
        gameId: game.gameId,
        elo: Math.min(game.whiteElo, game.blackElo), // Use lower ELO
      });
    }
  }

  // Limit data points for performance
  if (limit && points.length > limit) {
    // Sample evenly
    const step = Math.floor(points.length / limit);
    return points.filter((_, i) => i % step === 0).slice(0, limit);
  }

  return points;
}

/**
 * Group moves by move number range
 */
export function groupByMoveRange(games: GameData[]): Record<string, Move[]> {
  const ranges: Record<string, Move[]> = {
    'Opening (1-10)': [],
    'Middlegame (11-25)': [],
    'Endgame (26+)': [],
  };

  for (const game of games) {
    for (const move of game.moves) {
      const num = parseInt(move.moveNumber);
      if (num <= 10) {
        ranges['Opening (1-10)'].push(move);
      } else if (num <= 25) {
        ranges['Middlegame (11-25)'].push(move);
      } else {
        ranges['Endgame (26+)'].push(move);
      }
    }
  }

  return ranges;
}

/**
 * Format seconds to readable time
 */
export function formatTime(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;

  if (h > 0) {
    return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  }
  return `${m}:${s.toString().padStart(2, '0')}`;
}

/**
 * Format large numbers with commas
 */
export function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Get unique openings from games
 */
export function getUniqueOpenings(games: GameData[]): Array<{ eco: string; name: string; count: number }> {
  const openings = new Map<string, { name: string; count: number }>();

  for (const game of games) {
    const key = game.openingEco;
    if (openings.has(key)) {
      openings.get(key)!.count++;
    } else {
      openings.set(key, { name: game.openingName, count: 1 });
    }
  }

  return Array.from(openings.entries())
    .map(([eco, data]) => ({ eco, ...data }))
    .sort((a, b) => b.count - a.count);
}

/**
 * Calculate percentile for a value in a dataset
 */
export function calculatePercentile(value: number, dataset: number[]): number {
  const sorted = [...dataset].sort((a, b) => a - b);
  const index = sorted.findIndex(v => v >= value);
  if (index === -1) return 100;
  return (index / sorted.length) * 100;
}
