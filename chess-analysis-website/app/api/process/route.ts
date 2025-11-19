import { NextRequest, NextResponse } from 'next/server';
import { spawn } from 'child_process';
import { join } from 'path';
import { readFile, writeFile } from 'fs/promises';

interface ProcessRequest {
  sessionId: string;
  minElo?: number;
  maxGames?: number;
}

export async function POST(request: NextRequest) {
  try {
    const body: ProcessRequest = await request.json();
    const { sessionId, minElo = 1500, maxGames } = body;

    if (!sessionId) {
      return NextResponse.json(
        { error: 'Session ID is required' },
        { status: 400 }
      );
    }

    const inputPath = join(process.cwd(), 'uploads', sessionId, 'input.pgn');
    const outputPath = join(process.cwd(), 'uploads', sessionId, 'output.json');

    // Read PGN file
    const pgnContent = await readFile(inputPath, 'utf-8');

    // Process with Python (inline for simplicity)
    // In production, you might want to use a Python subprocess or FastAPI service
    const result = await processWithNode(pgnContent, minElo, maxGames);

    // Save result
    await writeFile(outputPath, JSON.stringify(result, null, 2));

    return NextResponse.json({
      sessionId,
      status: 'completed',
      summary: result.summary,
    });
  } catch (error) {
    console.error('Processing error:', error);
    return NextResponse.json(
      { error: 'Failed to process file', details: String(error) },
      { status: 500 }
    );
  }
}

// Node.js implementation of the Python processor
// This is a simplified version - for production, consider calling Python directly
async function processWithNode(
  content: string,
  minElo: number,
  maxGames?: number
): Promise<any> {
  const lines = content.split('\n');
  const games: any[] = [];
  let totalGames = 0;
  let processedGames = 0;
  let rejectedGames = 0;
  let totalMoves = 0;
  const timeControls = new Set<string>();
  let minEloSeen = Infinity;
  let maxEloSeen = 0;

  let currentGame: any = {
    metadata: {},
    moves: [],
    reject: false,
    hasClkEval: false,
  };

  for (const line of lines) {
    const trimmedLine = line.trim();

    // New game
    if (trimmedLine.startsWith('[Event ')) {
      if (totalGames > 0) {
        // Process previous game
        if (!currentGame.reject && currentGame.hasClkEval) {
          const gameData = extractGameData(currentGame, `game_${totalGames}`);
          if (gameData) {
            games.push(gameData);
            processedGames++;
            totalMoves += gameData.moves.length;
            timeControls.add(gameData.timeControl);
            minEloSeen = Math.min(minEloSeen, gameData.whiteElo, gameData.blackElo);
            maxEloSeen = Math.max(maxEloSeen, gameData.whiteElo, gameData.blackElo);
          }
        } else if (currentGame.reject) {
          rejectedGames++;
        }
      }

      totalGames++;
      currentGame = {
        metadata: {},
        moves: [],
        reject: false,
        hasClkEval: false,
      };

      if (maxGames && processedGames >= maxGames) {
        break;
      }
    }

    // Parse metadata
    if (trimmedLine.startsWith('[')) {
      const match = trimmedLine.match(/\[(\w+)\s+"([^"]+)"\]/);
      if (match) {
        const [, key, value] = match;
        currentGame.metadata[key] = value;

        // Check ELO filtering
        if (key === 'WhiteElo' || key === 'BlackElo') {
          const elo = parseInt(value, 10);
          if (isNaN(elo) || elo < minElo) {
            currentGame.reject = true;
          }
        }
      }
    }
    // Parse moves
    else if (trimmedLine && !currentGame.reject) {
      if (trimmedLine.includes('[%eval') && trimmedLine.includes('[%clk')) {
        currentGame.hasClkEval = true;
        currentGame.moves.push(trimmedLine);
      }
    }
  }

  // Process final game
  if (!currentGame.reject && currentGame.hasClkEval) {
    const gameData = extractGameData(currentGame, `game_${totalGames}`);
    if (gameData) {
      games.push(gameData);
      processedGames++;
      totalMoves += gameData.moves.length;
      timeControls.add(gameData.timeControl);
      minEloSeen = Math.min(minEloSeen, gameData.whiteElo, gameData.blackElo);
      maxEloSeen = Math.max(maxEloSeen, gameData.whiteElo, gameData.blackElo);
    }
  } else if (currentGame.reject) {
    rejectedGames++;
  }

  return {
    summary: {
      totalGames,
      processedGames,
      rejectedGames,
      totalMoves,
      timeControls: Array.from(timeControls).sort(),
      eloRange: {
        min: minEloSeen === Infinity ? 0 : minEloSeen,
        max: maxEloSeen,
      },
    },
    games,
  };
}

function extractGameData(game: any, gameId: string): any | null {
  try {
    const moves = parseMovesWithEval(game.moves.join(' '));

    return {
      gameId,
      whiteElo: parseInt(game.metadata.WhiteElo || '0', 10),
      blackElo: parseInt(game.metadata.BlackElo || '0', 10),
      timeControl: game.metadata.TimeControl || 'unknown',
      openingEco: game.metadata.ECO || 'unknown',
      openingName: game.metadata.Opening || 'unknown',
      result: game.metadata.Result || '*',
      date: game.metadata.UTCDate || 'unknown',
      moves,
    };
  } catch (error) {
    console.error('Error extracting game data:', error);
    return null;
  }
}

function parseMovesWithEval(movesText: string): any[] {
  const moves: any[] = [];
  const movePattern = /(\d+)\.\s*([^\{]+)\{\s*\[%eval\s+([^\]]+)\]\s*\[%clk\s+([^\]]+)\]\s*\}\s*(?:(\d+)\.{3}\s*)?([^\{]+)\{\s*\[%eval\s+([^\]]+)\]\s*\[%clk\s+([^\]]+)\]\s*\}/g;

  let match;
  let prevEval = 0;

  while ((match = movePattern.exec(movesText)) !== null) {
    const [, moveNum, , whiteEval, whiteClock, , , blackEval, blackClock] = match;

    const whiteEvalNum = parseEval(whiteEval.trim());
    const blackEvalNum = parseEval(blackEval.trim());

    // White's move
    const whiteCPL = Math.max(0, prevEval - whiteEvalNum);
    moves.push({
      moveNumber: `${moveNum}w`,
      eval: whiteEvalNum,
      centipawnLoss: whiteCPL,
      timeLeft: clockToSeconds(whiteClock.trim()),
      timeSpent: 0, // Simplified
      color: 'white',
    });

    // Black's move
    const blackCPL = Math.max(0, blackEvalNum - whiteEvalNum);
    moves.push({
      moveNumber: `${moveNum}b`,
      eval: blackEvalNum,
      centipawnLoss: blackCPL,
      timeLeft: clockToSeconds(blackClock.trim()),
      timeSpent: 0, // Simplified
      color: 'black',
    });

    prevEval = blackEvalNum;
  }

  return moves;
}

function parseEval(evalStr: string): number {
  if (evalStr.startsWith('#')) {
    return evalStr.includes('-') ? -10.0 : 10.0;
  }
  return parseFloat(evalStr);
}

function clockToSeconds(clockStr: string): number {
  const parts = clockStr.split(':').map(Number);
  if (parts.length === 3) {
    return parts[0] * 3600 + parts[1] * 60 + parts[2];
  } else if (parts.length === 2) {
    return parts[0] * 60 + parts[1];
  }
  return 0;
}

export const runtime = 'nodejs';
export const maxDuration = 300; // 5 minutes max
