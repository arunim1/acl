// Core data types for chess analysis

export interface GameMetadata {
  event?: string;
  site?: string;
  date?: string;
  round?: string;
  white?: string;
  black?: string;
  result?: string;
  whiteElo?: number;
  blackElo?: number;
  timeControl?: string;
  termination?: string;
  [key: string]: string | number | undefined;
}

export interface MoveData {
  moveNumber: number;
  side: 'w' | 'b';
  eval: number;
  centipawnLoss: number;
  timeLeft: number;
  timeSpent: number;
  move?: string;
}

export interface GameData {
  id: string;
  metadata: GameMetadata;
  moves: MoveData[];
}

export interface TimeControl {
  baseTime: number;  // in seconds
  increment: number; // in seconds
}

export interface FilterOptions {
  minElo?: number;
  maxElo?: number;
  timeControls?: string[];
  dateRange?: {
    start: Date;
    end: Date;
  };
  playerColor?: 'white' | 'black' | 'both';
  gameOutcome?: 'win' | 'loss' | 'draw' | 'all';
  movePhase?: 'opening' | 'middlegame' | 'endgame' | 'all';
}

export interface AnalysisResult {
  totalGames: number;
  totalMoves: number;
  avgCentipawnLoss: number;
  avgTimeSpent: number;
  timeVsLossData: Array<{
    timeSpent: number;
    avgLoss: number;
    stdDev: number;
    count: number;
  }>;
  moveDistribution: Array<{
    moveNumber: number;
    avgLoss: number;
    count: number;
  }>;
  eloImpact?: Array<{
    eloRange: string;
    avgLoss: number;
    count: number;
  }>;
}

export interface ProcessingProgress {
  gamesProcessed: number;
  totalGames: number;
  currentPhase: 'parsing' | 'analyzing' | 'complete';
  percentComplete: number;
}
