export interface Move {
  moveNumber: string;
  eval: number;
  centipawnLoss: number;
  timeLeft: number;
  timeSpent: number;
  color: 'white' | 'black';
}

export interface GameData {
  gameId: string;
  whiteElo: number;
  blackElo: number;
  timeControl: string;
  openingEco: string;
  openingName: string;
  result: string;
  date: string;
  moves: Move[];
}

export interface ProcessingSummary {
  totalGames: number;
  processedGames: number;
  rejectedGames: number;
  totalMoves: number;
  timeControls: string[];
  eloRange: {
    min: number;
    max: number;
  };
}

export interface ProcessedData {
  summary: ProcessingSummary;
  games: GameData[];
}

export interface UploadResponse {
  sessionId: string;
  fileName: string;
  fileSize: number;
  filePath: string;
}

export interface ProcessResponse {
  sessionId: string;
  status: 'completed' | 'processing' | 'error';
  summary?: ProcessingSummary;
  error?: string;
}

export interface FilterOptions {
  eloMin?: number;
  eloMax?: number;
  timeControls?: string[];
  openings?: string[];
  colors?: ('white' | 'black')[];
  dateRange?: {
    start: string;
    end: string;
  };
  result?: string[];
}

export interface ChartDataPoint {
  timeSpent: number;
  centipawnLoss: number;
  moveNumber: string;
  gameId: string;
  elo: number;
}

export interface AggregatedStats {
  averageCPL: number;
  medianCPL: number;
  blunderRate: number; // % moves with CPL > 200
  accuracyRate: number; // % moves with CPL < 50
  averageTimeSpent: number;
  totalMoves: number;
  gamesAnalyzed: number;
}
