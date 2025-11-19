'use client';

import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { Button } from '../ui/button';
import { useStore } from '@/lib/store';
import { AnalysisEngine } from '@/lib/analysis-engine';

export function FilterPanel() {
  const { filters, updateFilters, resetFilters, games, setAnalysisResult } = useStore();

  const handleFilterChange = (key: string, value: any) => {
    updateFilters({ [key]: value });
  };

  const handleApplyFilters = () => {
    if (games.length > 0) {
      const analysis = AnalysisEngine.analyze(games, filters);
      setAnalysisResult(analysis);
    }
  };

  const handleReset = () => {
    resetFilters();
    if (games.length > 0) {
      const analysis = AnalysisEngine.analyze(games);
      setAnalysisResult(analysis);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Filters</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* ELO Range */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Minimum ELO
          </label>
          <input
            type="number"
            value={filters.minElo || 2000}
            onChange={(e) => handleFilterChange('minElo', parseInt(e.target.value))}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            min="0"
            max="3000"
            step="100"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Maximum ELO
          </label>
          <input
            type="number"
            value={filters.maxElo || ''}
            onChange={(e) =>
              handleFilterChange('maxElo', e.target.value ? parseInt(e.target.value) : undefined)
            }
            placeholder="No limit"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            min="0"
            max="3000"
            step="100"
          />
        </div>

        {/* Player Color */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Player Color
          </label>
          <select
            value={filters.playerColor || 'both'}
            onChange={(e) => handleFilterChange('playerColor', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <option value="both">Both</option>
            <option value="white">White</option>
            <option value="black">Black</option>
          </select>
        </div>

        {/* Move Phase */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Game Phase
          </label>
          <select
            value={filters.movePhase || 'all'}
            onChange={(e) => handleFilterChange('movePhase', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <option value="all">All Phases</option>
            <option value="opening">Opening (1-15)</option>
            <option value="middlegame">Middlegame (16-40)</option>
            <option value="endgame">Endgame (40+)</option>
          </select>
        </div>

        {/* Action Buttons */}
        <div className="flex space-x-2 pt-4">
          <Button onClick={handleApplyFilters} className="flex-1">
            Apply Filters
          </Button>
          <Button onClick={handleReset} variant="outline" className="flex-1">
            Reset
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
