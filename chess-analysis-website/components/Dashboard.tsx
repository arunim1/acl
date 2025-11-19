'use client';

import { useState, useMemo } from 'react';
import { ProcessedData, FilterOptions } from '@/lib/types';
import { calculateStats, filterGames, filterMovesByColor } from '@/lib/utils';
import { InsightsDashboard } from './insights/InsightsDashboard';
import { CPLvsTimeScatter } from './charts/CPLvsTimeScatter';
import { CPLDistribution } from './charts/CPLDistribution';
import { MovePhaseChart } from './charts/MovePhaseChart';
import { OpeningAnalysis } from './charts/OpeningAnalysis';
import { FilterPanel } from './filters/FilterPanel';
import { DataExporter } from './export/DataExporter';
import { Card } from './ui/Card';
import { ChevronDown, ChevronUp } from 'lucide-react';

interface DashboardProps {
  data: ProcessedData;
  fileName: string;
}

export function Dashboard({ data, fileName }: DashboardProps) {
  const [filters, setFilters] = useState<FilterOptions>({});
  const [showFilters, setShowFilters] = useState(false);

  const filteredData = useMemo(() => {
    let games = filterGames(data.games, filters);
    if (filters.colors && filters.colors.length === 1) {
      games = filterMovesByColor(games, filters.colors[0]);
    }
    return games;
  }, [data.games, filters]);

  const stats = useMemo(() => calculateStats(filteredData, filters), [filteredData, filters]);

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold text-white mb-2">Analysis Results</h2>
          <p className="text-gray-400">
            {fileName} • {data.summary.processedGames.toLocaleString()} games •{' '}
            {data.summary.totalMoves.toLocaleString()} moves
          </p>
        </div>
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="flex items-center gap-2 px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition-colors"
        >
          {showFilters ? (
            <>
              <ChevronUp className="w-4 h-4" />
              Hide Filters
            </>
          ) : (
            <>
              <ChevronDown className="w-4 h-4" />
              Show Filters
            </>
          )}
        </button>
      </div>

      {/* Filters */}
      {showFilters && (
        <div className="animate-slide-in">
          <FilterPanel
            data={data}
            filters={filters}
            onFiltersChange={setFilters}
          />
        </div>
      )}

      {/* Insights Cards */}
      <InsightsDashboard stats={stats} summary={data.summary} />

      {/* Charts */}
      <div className="grid lg:grid-cols-2 gap-8">
        <Card title="Centipawn Loss vs Time Spent" description="Relationship between thinking time and move accuracy">
          <CPLvsTimeScatter games={filteredData} />
        </Card>

        <Card title="Centipawn Loss Distribution" description="Frequency of moves by accuracy level">
          <CPLDistribution games={filteredData} />
        </Card>

        <Card title="Accuracy by Game Phase" description="Average centipawn loss in opening, middlegame, and endgame">
          <MovePhaseChart games={filteredData} />
        </Card>

        <Card title="Opening Analysis" description="Top 10 openings by frequency and average accuracy">
          <OpeningAnalysis games={filteredData} />
        </Card>
      </div>

      {/* Data Export */}
      <DataExporter data={data} fileName={fileName} />

      {/* Summary Stats */}
      <Card>
        <div className="grid md:grid-cols-3 gap-6 text-center">
          <div>
            <div className="text-3xl font-bold text-chess-gold mb-1">
              {stats.gamesAnalyzed.toLocaleString()}
            </div>
            <div className="text-sm text-gray-400">Games Analyzed</div>
          </div>
          <div>
            <div className="text-3xl font-bold text-chess-gold mb-1">
              {stats.totalMoves.toLocaleString()}
            </div>
            <div className="text-sm text-gray-400">Total Moves</div>
          </div>
          <div>
            <div className="text-3xl font-bold text-chess-gold mb-1">
              {data.summary.eloRange.min} - {data.summary.eloRange.max}
            </div>
            <div className="text-sm text-gray-400">ELO Range</div>
          </div>
        </div>
      </Card>
    </div>
  );
}
