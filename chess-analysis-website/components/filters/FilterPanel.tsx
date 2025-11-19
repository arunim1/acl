'use client';

import { ProcessedData, FilterOptions } from '@/lib/types';
import { Card } from '../ui/Card';
import { X } from 'lucide-react';

interface FilterPanelProps {
  data: ProcessedData;
  filters: FilterOptions;
  onFiltersChange: (filters: FilterOptions) => void;
}

export function FilterPanel({ data, filters, onFiltersChange }: FilterPanelProps) {
  const updateFilter = (key: keyof FilterOptions, value: any) => {
    onFiltersChange({ ...filters, [key]: value });
  };

  const clearFilters = () => {
    onFiltersChange({});
  };

  const hasActiveFilters = Object.keys(filters).length > 0;

  return (
    <Card>
      <div className="flex items-center justify-between mb-6">
        <h3 className="text-lg font-semibold text-white">Filters</h3>
        {hasActiveFilters && (
          <button
            onClick={clearFilters}
            className="flex items-center gap-2 text-sm text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-4 h-4" />
            Clear All
          </button>
        )}
      </div>

      <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
        {/* ELO Range */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Min ELO
          </label>
          <input
            type="number"
            value={filters.eloMin || ''}
            onChange={(e) => updateFilter('eloMin', e.target.value ? parseInt(e.target.value) : undefined)}
            placeholder={`${data.summary.eloRange.min}`}
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:border-chess-green"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Max ELO
          </label>
          <input
            type="number"
            value={filters.eloMax || ''}
            onChange={(e) => updateFilter('eloMax', e.target.value ? parseInt(e.target.value) : undefined)}
            placeholder={`${data.summary.eloRange.max}`}
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:border-chess-green"
          />
        </div>

        {/* Time Control */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Time Control
          </label>
          <select
            value={filters.timeControls?.[0] || ''}
            onChange={(e) => updateFilter('timeControls', e.target.value ? [e.target.value] : undefined)}
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-chess-green"
          >
            <option value="">All</option>
            {data.summary.timeControls.map(tc => (
              <option key={tc} value={tc}>{tc}</option>
            ))}
          </select>
        </div>

        {/* Player Color */}
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-2">
            Player Color
          </label>
          <select
            value={filters.colors?.[0] || ''}
            onChange={(e) => updateFilter('colors', e.target.value ? [e.target.value as 'white' | 'black'] : undefined)}
            className="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:border-chess-green"
          >
            <option value="">Both</option>
            <option value="white">White</option>
            <option value="black">Black</option>
          </select>
        </div>
      </div>

      {hasActiveFilters && (
        <div className="mt-4 flex flex-wrap gap-2">
          {filters.eloMin && (
            <FilterTag label={`Min ELO: ${filters.eloMin}`} onRemove={() => updateFilter('eloMin', undefined)} />
          )}
          {filters.eloMax && (
            <FilterTag label={`Max ELO: ${filters.eloMax}`} onRemove={() => updateFilter('eloMax', undefined)} />
          )}
          {filters.timeControls && filters.timeControls.length > 0 && (
            <FilterTag label={`Time: ${filters.timeControls[0]}`} onRemove={() => updateFilter('timeControls', undefined)} />
          )}
          {filters.colors && filters.colors.length > 0 && (
            <FilterTag label={`Color: ${filters.colors[0]}`} onRemove={() => updateFilter('colors', undefined)} />
          )}
        </div>
      )}
    </Card>
  );
}

function FilterTag({ label, onRemove }: { label: string; onRemove: () => void }) {
  return (
    <div className="inline-flex items-center gap-2 px-3 py-1 bg-chess-green/20 border border-chess-green/50 rounded-full text-sm text-chess-green">
      <span>{label}</span>
      <button
        onClick={onRemove}
        className="hover:bg-chess-green/30 rounded-full p-0.5 transition-colors"
      >
        <X className="w-3 h-3" />
      </button>
    </div>
  );
}
