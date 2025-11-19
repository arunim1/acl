'use client';

import { Loader2, CheckCircle2 } from 'lucide-react';

interface ProcessingProgressProps {
  fileName: string;
}

export function ProcessingProgress({ fileName }: ProcessingProgressProps) {
  return (
    <div className="max-w-2xl mx-auto">
      <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-8">
        <div className="flex flex-col items-center text-center">
          <Loader2 className="w-16 h-16 text-chess-green animate-spin mb-6" />
          <h2 className="text-2xl font-bold text-white mb-2">Processing Your Games</h2>
          <p className="text-gray-400 mb-6">
            Analyzing <span className="text-chess-gold font-medium">{fileName}</span>
          </p>

          <div className="w-full space-y-4">
            <ProcessingStep
              label="Parsing PGN file"
              status="completed"
            />
            <ProcessingStep
              label="Extracting game data"
              status="completed"
            />
            <ProcessingStep
              label="Calculating centipawn loss"
              status="in-progress"
            />
            <ProcessingStep
              label="Generating insights"
              status="pending"
            />
          </div>

          <p className="text-sm text-gray-500 mt-8">
            This may take a few moments depending on file size...
          </p>
        </div>
      </div>
    </div>
  );
}

function ProcessingStep({
  label,
  status,
}: {
  label: string;
  status: 'completed' | 'in-progress' | 'pending';
}) {
  return (
    <div className="flex items-center gap-3 p-3 rounded-lg bg-gray-700/30">
      {status === 'completed' && (
        <CheckCircle2 className="w-5 h-5 text-green-500 flex-shrink-0" />
      )}
      {status === 'in-progress' && (
        <Loader2 className="w-5 h-5 text-chess-green animate-spin flex-shrink-0" />
      )}
      {status === 'pending' && (
        <div className="w-5 h-5 rounded-full border-2 border-gray-600 flex-shrink-0" />
      )}
      <span
        className={`text-left ${
          status === 'completed'
            ? 'text-white'
            : status === 'in-progress'
            ? 'text-chess-green font-medium'
            : 'text-gray-500'
        }`}
      >
        {label}
      </span>
    </div>
  );
}
