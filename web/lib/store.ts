// Global state management using Zustand

import { create } from 'zustand';
import type { GameData, FilterOptions, AnalysisResult } from './types';

interface AppState {
  // Data
  games: GameData[];
  analysisResult: AnalysisResult | null;

  // UI State
  isProcessing: boolean;
  processingProgress: number;
  selectedGameId: string | null;

  // Filters
  filters: FilterOptions;

  // Actions
  setGames: (games: GameData[]) => void;
  addGames: (games: GameData[]) => void;
  setAnalysisResult: (result: AnalysisResult | null) => void;
  setProcessing: (isProcessing: boolean, progress?: number) => void;
  setSelectedGame: (gameId: string | null) => void;
  updateFilters: (filters: Partial<FilterOptions>) => void;
  resetFilters: () => void;
  clearData: () => void;
}

const defaultFilters: FilterOptions = {
  minElo: 2000,
  playerColor: 'both',
  gameOutcome: 'all',
  movePhase: 'all',
};

export const useStore = create<AppState>((set) => ({
  // Initial state
  games: [],
  analysisResult: null,
  isProcessing: false,
  processingProgress: 0,
  selectedGameId: null,
  filters: defaultFilters,

  // Actions
  setGames: (games) => set({ games }),

  addGames: (newGames) =>
    set((state) => ({ games: [...state.games, ...newGames] })),

  setAnalysisResult: (result) => set({ analysisResult: result }),

  setProcessing: (isProcessing, progress = 0) =>
    set({ isProcessing, processingProgress: progress }),

  setSelectedGame: (gameId) => set({ selectedGameId: gameId }),

  updateFilters: (newFilters) =>
    set((state) => ({ filters: { ...state.filters, ...newFilters } })),

  resetFilters: () => set({ filters: defaultFilters }),

  clearData: () =>
    set({
      games: [],
      analysisResult: null,
      selectedGameId: null,
      processingProgress: 0,
    }),
}));
