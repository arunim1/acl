'use client';

import { ProcessedData } from '@/lib/types';
import { Download, FileJson, FileSpreadsheet } from 'lucide-react';
import { Button } from '../ui/Button';

interface DataExporterProps {
  data: ProcessedData;
  fileName: string;
}

export function DataExporter({ data, fileName }: DataExporterProps) {
  const exportToJSON = () => {
    const jsonString = JSON.stringify(data, null, 2);
    const blob = new Blob([jsonString], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${fileName.replace('.pgn', '')}-analysis.json`;
    link.click();
    URL.revokeObjectURL(url);
  };

  const exportToCSV = () => {
    // Create CSV from all moves
    const headers = ['Game ID', 'Move Number', 'Color', 'Evaluation', 'CPL', 'Time Left', 'Time Spent', 'White ELO', 'Black ELO', 'Time Control', 'Opening'];
    const rows = data.games.flatMap(game =>
      game.moves.map(move => [
        game.gameId,
        move.moveNumber,
        move.color,
        move.eval,
        move.centipawnLoss,
        move.timeLeft,
        move.timeSpent,
        game.whiteElo,
        game.blackElo,
        game.timeControl,
        game.openingEco,
      ])
    );

    const csvContent = [
      headers.join(','),
      ...rows.map(row => row.join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${fileName.replace('.pgn', '')}-analysis.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-6">
      <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
        <Download className="w-5 h-5" />
        Export Data
      </h3>
      <div className="flex flex-wrap gap-3">
        <Button onClick={exportToJSON} variant="outline" icon={<FileJson className="w-4 h-4" />}>
          Export as JSON
        </Button>
        <Button onClick={exportToCSV} variant="outline" icon={<FileSpreadsheet className="w-4 h-4" />}>
          Export as CSV
        </Button>
      </div>
      <p className="text-sm text-gray-400 mt-3">
        Download your analysis results for further processing or sharing
      </p>
    </div>
  );
}
