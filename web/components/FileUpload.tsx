'use client';

import React, { useCallback, useState } from 'react';
import { Upload, FileText, X } from 'lucide-react';
import { Button } from './ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Progress } from './ui/progress';
import { Alert, AlertDescription, AlertTitle } from './ui/alert';
import { ChessParser } from '@/lib/chess-parser';
import { AnalysisEngine } from '@/lib/analysis-engine';
import { useStore } from '@/lib/store';

export function FileUpload() {
  const [isDragging, setIsDragging] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);
  const { setGames, setAnalysisResult, setProcessing, isProcessing, processingProgress } =
    useStore();

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    setError(null);

    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile && droppedFile.name.endsWith('.pgn')) {
      setFile(droppedFile);
    } else {
      setError('Please upload a valid PGN file');
    }
  }, []);

  const handleFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0];
    setError(null);
    if (selectedFile) {
      if (selectedFile.name.endsWith('.pgn')) {
        setFile(selectedFile);
      } else {
        setError('Please select a valid PGN file');
      }
    }
  }, []);

  const handleProcess = useCallback(async () => {
    if (!file) return;

    try {
      setError(null);
      setProcessing(true, 0);

      // Parse the PGN file
      const games = await ChessParser.parsePGNFile(file, {
        requireClkEval: true,
        minElo: 2000,
        onProgress: (progress) => {
          setProcessing(true, progress.percentComplete);
        },
      });

      if (games.length === 0) {
        throw new Error(
          'No valid games found. Ensure the PGN file contains games with both %clk and %eval annotations, and players with ELO >= 2000.'
        );
      }

      // Store games
      setGames(games);

      // Perform analysis
      const analysis = AnalysisEngine.analyze(games);
      setAnalysisResult(analysis);

      setProcessing(false, 100);
    } catch (error) {
      console.error('Error processing file:', error);
      setError(error instanceof Error ? error.message : 'Failed to process file');
      setProcessing(false, 0);
    }
  }, [file, setGames, setAnalysisResult, setProcessing]);

  const handleClear = useCallback(() => {
    setFile(null);
    setError(null);
  }, []);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Upload PGN File</CardTitle>
        <CardDescription>
          Upload a PGN file with clock and evaluation data to analyze centipawn loss patterns
        </CardDescription>
      </CardHeader>
      <CardContent>
        {error && (
          <Alert variant="error" className="mb-4">
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}
        {!file ? (
          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            className={`
              border-2 border-dashed rounded-lg p-12 text-center cursor-pointer
              transition-colors duration-200
              ${
                isDragging
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-300 hover:border-gray-400'
              }
            `}
          >
            <Upload className="mx-auto h-12 w-12 text-gray-400 mb-4" />
            <p className="text-lg font-medium text-gray-700 mb-2">
              Drag and drop your PGN file here
            </p>
            <p className="text-sm text-gray-500 mb-4">or</p>
            <label htmlFor="file-upload">
              <Button type="button" onClick={() => document.getElementById('file-upload')?.click()}>
                Browse Files
              </Button>
              <input
                id="file-upload"
                type="file"
                accept=".pgn"
                className="hidden"
                onChange={handleFileInput}
              />
            </label>
            <p className="text-xs text-gray-400 mt-4">
              PGN files with %clk and %eval annotations required
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
              <div className="flex items-center space-x-3">
                <FileText className="h-8 w-8 text-blue-600" />
                <div>
                  <p className="font-medium text-gray-900">{file.name}</p>
                  <p className="text-sm text-gray-500">
                    {(file.size / 1024 / 1024).toFixed(2)} MB
                  </p>
                </div>
              </div>
              <Button
                variant="ghost"
                size="icon"
                onClick={handleClear}
                disabled={isProcessing}
              >
                <X className="h-5 w-5" />
              </Button>
            </div>

            {isProcessing && (
              <div className="space-y-2">
                <div className="flex justify-between text-sm text-gray-600">
                  <span>Processing...</span>
                  <span>{processingProgress}%</span>
                </div>
                <Progress value={processingProgress} />
              </div>
            )}

            <Button
              onClick={handleProcess}
              disabled={isProcessing}
              className="w-full"
            >
              {isProcessing ? 'Processing...' : 'Analyze Games'}
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
