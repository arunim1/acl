// Analysis engine for computing statistics on chess game data

import type { GameData, MoveData, AnalysisResult, FilterOptions } from './types';

export class AnalysisEngine {
  /**
   * Filter games based on provided criteria
   */
  static filterGames(games: GameData[], filters: FilterOptions): GameData[] {
    return games.filter((game) => {
      // ELO filter
      if (filters.minElo !== undefined) {
        const minElo = Math.min(
          game.metadata.whiteElo || 0,
          game.metadata.blackElo || 0
        );
        if (minElo < filters.minElo) return false;
      }

      if (filters.maxElo !== undefined) {
        const maxElo = Math.max(
          game.metadata.whiteElo || 0,
          game.metadata.blackElo || 0
        );
        if (maxElo > filters.maxElo) return false;
      }

      // Time control filter
      if (filters.timeControls && filters.timeControls.length > 0) {
        if (!filters.timeControls.includes(game.metadata.timeControl || '')) {
          return false;
        }
      }

      // Date range filter
      if (filters.dateRange) {
        const gameDate = new Date(game.metadata.date || '');
        if (
          gameDate < filters.dateRange.start ||
          gameDate > filters.dateRange.end
        ) {
          return false;
        }
      }

      // Game outcome filter
      if (filters.gameOutcome && filters.gameOutcome !== 'all') {
        // This would require tracking which side we're analyzing
        // For now, skip this filter
      }

      return true;
    });
  }

  /**
   * Filter moves based on criteria
   */
  static filterMoves(moves: MoveData[], filters: FilterOptions): MoveData[] {
    return moves.filter((move) => {
      // Player color filter
      if (filters.playerColor && filters.playerColor !== 'both') {
        if (filters.playerColor === 'white' && move.side !== 'w') return false;
        if (filters.playerColor === 'black' && move.side !== 'b') return false;
      }

      // Move phase filter
      if (filters.movePhase && filters.movePhase !== 'all') {
        if (filters.movePhase === 'opening' && move.moveNumber > 15) return false;
        if (
          filters.movePhase === 'middlegame' &&
          (move.moveNumber <= 15 || move.moveNumber > 40)
        )
          return false;
        if (filters.movePhase === 'endgame' && move.moveNumber <= 40) return false;
      }

      return true;
    });
  }

  /**
   * Calculate aggregated time vs loss data
   */
  static calculateTimeVsLoss(
    moves: MoveData[],
    maxTime?: number
  ): Array<{
    timeSpent: number;
    avgLoss: number;
    stdDev: number;
    count: number;
  }> {
    // Group moves by time spent (rounded to nearest second)
    const timeGroups = new Map<
      number,
      { losses: number[]; count: number }
    >();

    for (const move of moves) {
      const timeSpent = Math.round(move.timeSpent);

      // Skip if beyond max time
      if (maxTime !== undefined && timeSpent > maxTime) continue;

      // Skip invalid data
      if (timeSpent < 0 || move.centipawnLoss > 10) continue;

      if (!timeGroups.has(timeSpent)) {
        timeGroups.set(timeSpent, { losses: [], count: 0 });
      }

      const group = timeGroups.get(timeSpent)!;
      group.losses.push(move.centipawnLoss);
      group.count++;
    }

    // Calculate statistics for each time group
    const result: Array<{
      timeSpent: number;
      avgLoss: number;
      stdDev: number;
      count: number;
    }> = [];

    for (const [timeSpent, group] of timeGroups.entries()) {
      if (group.count < 10) continue; // Skip groups with insufficient data

      const avgLoss =
        group.losses.reduce((sum, loss) => sum + loss, 0) / group.count;

      const variance =
        group.losses.reduce(
          (sum, loss) => sum + Math.pow(loss - avgLoss, 2),
          0
        ) / group.count;

      const stdDev = Math.sqrt(variance);

      result.push({
        timeSpent,
        avgLoss,
        stdDev,
        count: group.count,
      });
    }

    // Sort by time spent
    return result.sort((a, b) => a.timeSpent - b.timeSpent);
  }

  /**
   * Calculate move number distribution
   */
  static calculateMoveDistribution(
    moves: MoveData[]
  ): Array<{
    moveNumber: number;
    avgLoss: number;
    count: number;
  }> {
    const moveGroups = new Map<
      number,
      { losses: number[]; count: number }
    >();

    for (const move of moves) {
      if (move.centipawnLoss > 10) continue; // Skip outliers

      if (!moveGroups.has(move.moveNumber)) {
        moveGroups.set(move.moveNumber, { losses: [], count: 0 });
      }

      const group = moveGroups.get(move.moveNumber)!;
      group.losses.push(move.centipawnLoss);
      group.count++;
    }

    const result: Array<{
      moveNumber: number;
      avgLoss: number;
      count: number;
    }> = [];

    for (const [moveNumber, group] of moveGroups.entries()) {
      if (group.count < 5) continue;

      const avgLoss =
        group.losses.reduce((sum, loss) => sum + loss, 0) / group.count;

      result.push({
        moveNumber,
        avgLoss,
        count: group.count,
      });
    }

    return result.sort((a, b) => a.moveNumber - b.moveNumber);
  }

  /**
   * Calculate ELO impact on accuracy
   */
  static calculateEloImpact(
    games: GameData[],
    binSize = 200
  ): Array<{
    eloRange: string;
    avgLoss: number;
    count: number;
  }> {
    const eloBins = new Map<string, { losses: number[]; count: number }>();

    for (const game of games) {
      const avgElo = Math.floor(
        ((game.metadata.whiteElo || 0) + (game.metadata.blackElo || 0)) / 2
      );
      const binStart = Math.floor(avgElo / binSize) * binSize;
      const binEnd = binStart + binSize;
      const binKey = `${binStart}-${binEnd}`;

      if (!eloBins.has(binKey)) {
        eloBins.set(binKey, { losses: [], count: 0 });
      }

      const bin = eloBins.get(binKey)!;

      for (const move of game.moves) {
        if (move.centipawnLoss <= 10) {
          bin.losses.push(move.centipawnLoss);
          bin.count++;
        }
      }
    }

    const result: Array<{
      eloRange: string;
      avgLoss: number;
      count: number;
    }> = [];

    for (const [eloRange, bin] of eloBins.entries()) {
      if (bin.count < 50) continue;

      const avgLoss =
        bin.losses.reduce((sum, loss) => sum + loss, 0) / bin.count;

      result.push({
        eloRange,
        avgLoss,
        count: bin.count,
      });
    }

    return result.sort((a, b) => {
      const aStart = parseInt(a.eloRange.split('-')[0]);
      const bStart = parseInt(b.eloRange.split('-')[0]);
      return aStart - bStart;
    });
  }

  /**
   * Perform comprehensive analysis on game data
   */
  static analyze(games: GameData[], filters?: FilterOptions): AnalysisResult {
    // Apply filters if provided
    const filteredGames = filters
      ? this.filterGames(games, filters)
      : games;

    // Collect all moves
    const allMoves = filteredGames.flatMap((game) => game.moves);

    // Apply move filters if provided
    const filteredMoves = filters
      ? this.filterMoves(allMoves, filters)
      : allMoves;

    // Calculate basic statistics
    const totalGames = filteredGames.length;
    const totalMoves = filteredMoves.length;

    const avgCentipawnLoss =
      filteredMoves.reduce((sum, move) => sum + move.centipawnLoss, 0) /
      totalMoves;

    const avgTimeSpent =
      filteredMoves.reduce((sum, move) => sum + move.timeSpent, 0) /
      totalMoves;

    // Calculate advanced metrics
    const timeVsLossData = this.calculateTimeVsLoss(filteredMoves);
    const moveDistribution = this.calculateMoveDistribution(filteredMoves);
    const eloImpact = this.calculateEloImpact(filteredGames);

    return {
      totalGames,
      totalMoves,
      avgCentipawnLoss,
      avgTimeSpent,
      timeVsLossData,
      moveDistribution,
      eloImpact,
    };
  }

  /**
   * Export analysis results to CSV format
   */
  static exportToCSV(games: GameData[]): string {
    const headers = [
      'Game ID',
      'White',
      'Black',
      'White ELO',
      'Black ELO',
      'Time Control',
      'Move Number',
      'Side',
      'Eval',
      'Centipawn Loss',
      'Time Left',
      'Time Spent',
      'Move',
    ];

    const rows: string[] = [headers.join(',')];

    for (const game of games) {
      for (const move of game.moves) {
        const row = [
          game.id,
          game.metadata.white || '',
          game.metadata.black || '',
          game.metadata.whiteElo || '',
          game.metadata.blackElo || '',
          game.metadata.timeControl || '',
          move.moveNumber,
          move.side,
          move.eval.toFixed(2),
          move.centipawnLoss.toFixed(2),
          move.timeLeft,
          move.timeSpent.toFixed(2),
          move.move || '',
        ];
        rows.push(row.join(','));
      }
    }

    return rows.join('\n');
  }
}
