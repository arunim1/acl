'use client';

import React, { useEffect } from 'react';
import { FileUpload } from '@/components/FileUpload';
import { FilterPanel } from '@/components/filters/FilterPanel';
import { StatsOverview } from '@/components/StatsOverview';
import { TimeVsLossChart } from '@/components/charts/TimeVsLossChart';
import { MoveDistributionChart } from '@/components/charts/MoveDistributionChart';
import { EloImpactChart } from '@/components/charts/EloImpactChart';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Download, Github } from 'lucide-react';
import { useStore } from '@/lib/store';
import { AnalysisEngine } from '@/lib/analysis-engine';
import { downloadFile } from '@/lib/utils';
import { ChessParser } from '@/lib/chess-parser';

export default function Home() {
  const { games, analysisResult, setGames, setAnalysisResult } = useStore();

  const loadSampleData = React.useCallback(async () => {
    // Load the sample.pgn file as sample data
    try {
      const response = await fetch('/sample.pgn');
      const text = await response.text();

      const sampleGames = ChessParser.parsePGNText(text, {
        requireClkEval: true,
        minElo: 2000,
      });

      if (sampleGames.length > 0) {
        setGames(sampleGames);
        const analysis = AnalysisEngine.analyze(sampleGames);
        setAnalysisResult(analysis);
      }
    } catch (error) {
      console.error('Error loading sample data:', error);
    }
  }, [setGames, setAnalysisResult]);

  // Load sample data on mount
  useEffect(() => {
    loadSampleData();
  }, [loadSampleData]);

  const handleExportCSV = () => {
    if (games.length === 0) return;

    const csv = AnalysisEngine.exportToCSV(games);
    downloadFile(csv, 'chess-analysis.csv', 'text/csv');
  };

  const hasData = analysisResult !== null;

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                Centipawn Loss Analyzer
              </h1>
              <p className="mt-1 text-sm text-gray-500">
                Explore the relationship between thinking time and move quality
              </p>
            </div>
            <a
              href="https://github.com"
              target="_blank"
              rel="noopener noreferrer"
              className="text-gray-600 hover:text-gray-900"
            >
              <Github className="h-6 w-6" />
            </a>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Left Sidebar - Upload and Filters */}
          <div className="lg:col-span-1 space-y-6">
            <FileUpload />
            {hasData && <FilterPanel />}
          </div>

          {/* Main Content Area */}
          <div className="lg:col-span-3 space-y-6">
            {hasData ? (
              <>
                {/* Stats Overview */}
                <StatsOverview analysisResult={analysisResult} />

                {/* Export Button */}
                <div className="flex justify-end">
                  <Button onClick={handleExportCSV} variant="outline">
                    <Download className="h-4 w-4 mr-2" />
                    Export CSV
                  </Button>
                </div>

                {/* Time vs Loss Chart */}
                <Card>
                  <CardHeader>
                    <CardTitle>Time Spent vs Centipawn Loss</CardTitle>
                    <CardDescription>
                      Relationship between thinking time and move accuracy
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <TimeVsLossChart
                      data={analysisResult.timeVsLossData}
                      maxTime={60}
                    />
                  </CardContent>
                </Card>

                {/* Move Distribution Chart */}
                <Card>
                  <CardHeader>
                    <CardTitle>Move Number Distribution</CardTitle>
                    <CardDescription>
                      Average centipawn loss throughout the game
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <MoveDistributionChart
                      data={analysisResult.moveDistribution}
                      maxMoves={60}
                    />
                  </CardContent>
                </Card>

                {/* ELO Impact Chart */}
                {analysisResult.eloImpact && (
                  <Card>
                    <CardHeader>
                      <CardTitle>ELO Impact on Accuracy</CardTitle>
                      <CardDescription>
                        How player rating affects move quality
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <EloImpactChart data={analysisResult.eloImpact} />
                    </CardContent>
                  </Card>
                )}
              </>
            ) : (
              <Card>
                <CardContent className="flex flex-col items-center justify-center py-16">
                  <div className="text-center max-w-md">
                    <h3 className="text-lg font-semibold text-gray-900 mb-2">
                      No Data Loaded
                    </h3>
                    <p className="text-gray-600 mb-6">
                      Upload a PGN file with clock and evaluation annotations to begin analyzing
                      chess games. The file should include %clk and %eval tags for each move.
                    </p>
                    <Button onClick={loadSampleData}>
                      Load Sample Data
                    </Button>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-gray-200 mt-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <p className="text-center text-sm text-gray-500">
            Built with Next.js and Recharts • Data from Lichess Database
          </p>
        </div>
      </footer>
    </div>
  );
}
