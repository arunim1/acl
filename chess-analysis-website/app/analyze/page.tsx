'use client';

import { useState } from 'react';
import { FileUploader } from '@/components/upload/FileUploader';
import { ProcessingProgress } from '@/components/upload/ProcessingProgress';
import { Dashboard } from '@/components/Dashboard';
import { UploadResponse, ProcessResponse, ProcessedData } from '@/lib/types';
import { ArrowLeft } from 'lucide-react';
import Link from 'next/link';

type ViewState = 'upload' | 'processing' | 'analyzing';

export default function AnalyzePage() {
  const [viewState, setViewState] = useState<ViewState>('upload');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [processedData, setProcessedData] = useState<ProcessedData | null>(null);
  const [fileName, setFileName] = useState<string>('');

  const handleUploadComplete = async (response: UploadResponse) => {
    setSessionId(response.sessionId);
    setFileName(response.fileName);
    setViewState('processing');

    // Start processing
    try {
      const processResponse = await fetch('/api/process', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          sessionId: response.sessionId,
          minElo: 1500,
        }),
      });

      if (!processResponse.ok) {
        throw new Error('Processing failed');
      }

      const processData: ProcessResponse = await processResponse.json();

      // Fetch processed data
      const dataResponse = await fetch(`/api/data/${response.sessionId}`);
      if (!dataResponse.ok) {
        throw new Error('Failed to fetch processed data');
      }

      const data: ProcessedData = await dataResponse.json();
      setProcessedData(data);
      setViewState('analyzing');
    } catch (error) {
      console.error('Processing error:', error);
      alert('Failed to process file. Please try again.');
      setViewState('upload');
    }
  };

  const handleReset = () => {
    setViewState('upload');
    setSessionId(null);
    setProcessedData(null);
    setFileName('');
  };

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-900 via-gray-800 to-gray-900">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur-sm sticky top-0 z-50">
        <div className="container mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link
                href="/"
                className="flex items-center gap-2 text-gray-400 hover:text-white transition-colors"
              >
                <ArrowLeft className="w-5 h-5" />
                <span>Back</span>
              </Link>
              <h1 className="text-2xl font-bold bg-gradient-to-r from-chess-gold to-chess-green bg-clip-text text-transparent">
                ChessInsights
              </h1>
            </div>
            {viewState === 'analyzing' && (
              <button
                onClick={handleReset}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition-colors"
              >
                Analyze New File
              </button>
            )}
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        {viewState === 'upload' && (
          <div className="animate-slide-in">
            <div className="text-center mb-12">
              <h2 className="text-4xl font-bold text-white mb-4">
                Upload Your Chess Games
              </h2>
              <p className="text-lg text-gray-400">
                Analyze your PGN files to discover patterns in move quality and time management
              </p>
            </div>
            <FileUploader onUploadComplete={handleUploadComplete} />
          </div>
        )}

        {viewState === 'processing' && sessionId && (
          <div className="animate-slide-in">
            <ProcessingProgress fileName={fileName} />
          </div>
        )}

        {viewState === 'analyzing' && processedData && (
          <div className="animate-slide-in">
            <Dashboard data={processedData} fileName={fileName} />
          </div>
        )}
      </main>
    </div>
  );
}
