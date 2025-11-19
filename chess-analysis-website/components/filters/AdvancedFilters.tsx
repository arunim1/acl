'use client';

import { FilterOptions } from '@/lib/types';
import { Card } from '../ui/Card';

interface AdvancedFiltersProps {
  filters: FilterOptions;
  onFiltersChange: (filters: FilterOptions) => void;
}

export function AdvancedFilters({ filters, onFiltersChange }: AdvancedFiltersProps) {
  const updateFilter = (key: keyof FilterOptions, value: any) => {
    onFiltersChange({ ...filters, [key]: value });
  };

  return (
    <Card title="Advanced Filters" className="mt-6">
      <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Result Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Game Result
          </label>
          <select
            value={filters.result?.[0] || ''}
            onChange={(e) => updateFilter('result', e.target.value ? [e.target.value] : undefined)}
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-chess-green"
          >
            <option value="">All Results</option>
            <option value="1-0">White Wins</option>
            <option value="0-1">Black Wins</option>
            <option value="1/2-1/2">Draws</option>
          </select>
        </div>

        {/* Move Range Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Game Phase
          </label>
          <select
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-chess-green"
          >
            <option value="">All Phases</option>
            <option value="opening">Opening (1-10)</option>
            <option value="middlegame">Middlegame (11-25)</option>
            <option value="endgame">Endgame (26+)</option>
          </select>
        </div>

        {/* Max CPL Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Max Centipawn Loss
          </label>
          <input
            type="number"
            placeholder="300"
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:border-chess-green"
          />
        </div>
      </div>
    </Card>
  );
}
