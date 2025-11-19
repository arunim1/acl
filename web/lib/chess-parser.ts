// Chess PGN parser optimized for browser performance

import type { GameMetadata, MoveData, GameData, TimeControl } from './types';

export class GameState {
  moves: string[] = [];
  metadata: GameMetadata = {};
  hasClkEval = false;
  reject = false;
  prevWhiteClock: number | null = null;
  prevBlackClock: number | null = null;
  prevEval = 0.0;
}

export class ChessParser {
  // Precompiled regex patterns
  private static readonly MOVE_PATTERN = /(\d+)\.\s*([^\{]+)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}\s*([^\{]*)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}/g;
  private static readonly METADATA_PATTERN = /\[(\w+)\s+"([^"]+)"\]/;
  private static readonly EVENT_START = '[Event ';

  /**
   * Convert clock time string (HH:MM:SS or MM:SS) to total seconds
   */
  static clockToSeconds(clockStr: string): number {
    const parts = clockStr.trim().split(':');
    if (parts.length === 2) {
      // MM:SS format
      return parseInt(parts[0]) * 60 + parseInt(parts[1]);
    } else if (parts.length === 3) {
      // HH:MM:SS format
      return parseInt(parts[0]) * 3600 + parseInt(parts[1]) * 60 + parseInt(parts[2]);
    }
    return 0;
  }

  /**
   * Convert evaluation string to float, handling mate scores
   */
  static parseEval(evalStr: string): number {
    evalStr = evalStr.trim();
    if (evalStr.startsWith('#')) {
      return evalStr.startsWith('#-') ? -10.0 : 10.0;
    }
    return parseFloat(evalStr);
  }

  /**
   * Parse time control string into initial time and increment
   */
  static parseTimeControl(tcStr: string): TimeControl {
    const parts = tcStr.split('+');
    return {
      baseTime: parseInt(parts[0]) || 0,
      increment: parseInt(parts[1]) || 0,
    };
  }

  /**
   * Process moves for a single game and return structured move data
   */
  static processMoves(state: GameState, minElo = 2000): MoveData[] {
    if (!state.moves.length || !state.hasClkEval || state.reject) {
      return [];
    }

    // Check ELO requirements
    if (state.metadata.whiteElo && state.metadata.whiteElo < minElo) {
      return [];
    }
    if (state.metadata.blackElo && state.metadata.blackElo < minElo) {
      return [];
    }

    const timeControl = this.parseTimeControl(state.metadata.timeControl || '0+0');
    const moveData: MoveData[] = [];

    for (const moveLine of state.moves) {
      // Reset regex lastIndex
      this.MOVE_PATTERN.lastIndex = 0;

      let match;
      while ((match = this.MOVE_PATTERN.exec(moveLine)) !== null) {
        const [
          ,
          moveNumStr,
          whiteMove,
          whiteEvalStr,
          whiteClockStr,
          blackMove,
          blackEvalStr,
          blackClockStr,
        ] = match;

        const moveNum = parseInt(moveNumStr);

        // Process white's move
        const whiteClockSecs = this.clockToSeconds(whiteClockStr);
        const whiteTimeSpent =
          state.prevWhiteClock === null
            ? timeControl.baseTime - whiteClockSecs
            : state.prevWhiteClock - whiteClockSecs + timeControl.increment;

        const whiteEval = this.parseEval(whiteEvalStr);
        moveData.push({
          moveNumber: moveNum,
          side: 'w',
          eval: whiteEval,
          centipawnLoss: Math.max(0, state.prevEval - whiteEval),
          timeLeft: whiteClockSecs,
          timeSpent: Math.max(0, whiteTimeSpent),
          move: whiteMove.trim(),
        });
        state.prevEval = whiteEval;
        state.prevWhiteClock = whiteClockSecs;

        // Process black's move if it exists
        if (blackMove && blackMove.trim()) {
          const blackClockSecs = this.clockToSeconds(blackClockStr);
          const blackTimeSpent =
            state.prevBlackClock === null
              ? timeControl.baseTime - blackClockSecs
              : state.prevBlackClock - blackClockSecs + timeControl.increment;

          const blackEval = this.parseEval(blackEvalStr);
          moveData.push({
            moveNumber: moveNum,
            side: 'b',
            eval: blackEval,
            centipawnLoss: Math.max(0, blackEval - state.prevEval),
            timeLeft: blackClockSecs,
            timeSpent: Math.max(0, blackTimeSpent),
            move: blackMove.trim(),
          });
          state.prevEval = blackEval;
          state.prevBlackClock = blackClockSecs;
        }
      }
    }

    return moveData;
  }

  /**
   * Check if a game should be rejected based on metadata
   */
  static shouldRejectGame(metadata: GameMetadata, minElo = 2000): boolean {
    if (metadata.whiteElo && metadata.whiteElo < minElo) return true;
    if (metadata.blackElo && metadata.blackElo < minElo) return true;
    if (metadata.termination && metadata.termination.includes('Abandoned')) return true;
    return false;
  }

  /**
   * Parse a single line of PGN metadata
   */
  static parseMetadataLine(line: string): { key: string; value: string } | null {
    const match = line.match(this.METADATA_PATTERN);
    if (!match) return null;

    const [, key, value] = match;
    return { key, value };
  }

  /**
   * Process a chunk of PGN text and yield complete games
   */
  static *processChunk(
    lines: string[],
    state: GameState,
    requireClkEval = true,
    minElo = 2000
  ): Generator<GameData> {
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      const firstChar = trimmed[0];

      if (firstChar === '[') {
        if (trimmed.startsWith(this.EVENT_START)) {
          // New game starting - process previous game if valid
          if (state.metadata.timeControl && (!requireClkEval || state.hasClkEval)) {
            const moves = this.processMoves(state, minElo);
            if (moves.length > 0) {
              yield {
                id: `${state.metadata.white}_${state.metadata.black}_${state.metadata.date}_${state.metadata.timeControl}`,
                metadata: { ...state.metadata },
                moves,
              };
            }
          }
          // Reset state for new game
          state = new GameState();
        }

        // Parse metadata
        const metadata = this.parseMetadataLine(trimmed);
        if (metadata) {
          const { key, value } = metadata;

          // Convert numeric fields
          if (key === 'WhiteElo' || key === 'BlackElo') {
            const eloValue = parseInt(value);
            state.metadata[key.charAt(0).toLowerCase() + key.slice(1) as keyof GameMetadata] = eloValue;
          } else {
            state.metadata[key.charAt(0).toLowerCase() + key.slice(1) as keyof GameMetadata] = value;
          }

          // Quick reject based on metadata
          if (requireClkEval && this.shouldRejectGame(state.metadata, minElo)) {
            state.reject = true;
          }
        }
      } else {
        // Move lines
        if (trimmed.includes('%clk') && trimmed.includes('%eval')) {
          state.hasClkEval = true;
          state.moves.push(trimmed);
        }
      }
    }

    // Process final game
    if (state.metadata.timeControl && (!requireClkEval || state.hasClkEval)) {
      const moves = this.processMoves(state, minElo);
      if (moves.length > 0) {
        yield {
          id: `${state.metadata.white}_${state.metadata.black}_${state.metadata.date}_${state.metadata.timeControl}`,
          metadata: { ...state.metadata },
          moves,
        };
      }
    }
  }

  /**
   * Parse an entire PGN file
   */
  static async parsePGNFile(
    file: File,
    options: {
      requireClkEval?: boolean;
      minElo?: number;
      onProgress?: (progress: { gamesProcessed: number; percentComplete: number }) => void;
    } = {}
  ): Promise<GameData[]> {
    const { requireClkEval = true, minElo = 2000, onProgress } = options;

    const games: GameData[] = [];
    const text = await file.text();
    const lines = text.split('\n');
    const totalLines = lines.length;

    let state = new GameState();
    const chunkSize = 1000; // Process in chunks of 1000 lines

    for (let i = 0; i < lines.length; i += chunkSize) {
      const chunk = lines.slice(i, i + chunkSize);
      const chunkGames = Array.from(this.processChunk(chunk, state, requireClkEval, minElo));
      games.push(...chunkGames);

      if (onProgress) {
        onProgress({
          gamesProcessed: games.length,
          percentComplete: Math.min(100, Math.floor((i / totalLines) * 100)),
        });
      }

      // Allow UI to update
      await new Promise((resolve) => setTimeout(resolve, 0));
    }

    return games;
  }

  /**
   * Parse PGN text from a string
   */
  static parsePGNText(
    text: string,
    options: {
      requireClkEval?: boolean;
      minElo?: number;
    } = {}
  ): GameData[] {
    const { requireClkEval = true, minElo = 2000 } = options;

    const lines = text.split('\n');
    const state = new GameState();

    return Array.from(this.processChunk(lines, state, requireClkEval, minElo));
  }
}
