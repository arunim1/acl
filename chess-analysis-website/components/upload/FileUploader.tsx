'use client';

import { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, File, X, AlertCircle } from 'lucide-react';
import { UploadResponse } from '@/lib/types';

interface FileUploaderProps {
  onUploadComplete: (response: UploadResponse) => void;
}

export function FileUploader({ onUploadComplete }: FileUploaderProps) {
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      setSelectedFile(acceptedFiles[0]);
      setError(null);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/x-chess-pgn': ['.pgn'],
      'text/plain': ['.pgn'],
    },
    maxFiles: 1,
    multiple: false,
  });

  const handleUpload = async () => {
    if (!selectedFile) return;

    setUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('file', selectedFile);

      const response = await fetch('/api/upload', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || 'Upload failed');
      }

      const data: UploadResponse = await response.json();
      onUploadComplete(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

  const clearFile = () => {
    setSelectedFile(null);
    setError(null);
  };

  return (
    <div className="w-full max-w-2xl mx-auto">
      <div
        {...getRootProps()}
        className={`border-2 border-dashed rounded-lg p-12 text-center cursor-pointer transition-all ${
          isDragActive
            ? 'border-chess-green bg-chess-green/10'
            : 'border-gray-600 hover:border-chess-green/50'
        } ${selectedFile ? 'bg-gray-800/30' : 'bg-gray-800/50'}`}
      >
        <input {...getInputProps()} />
        <Upload className={`w-16 h-16 mx-auto mb-4 ${isDragActive ? 'text-chess-green' : 'text-gray-400'}`} />
        {isDragActive ? (
          <p className="text-lg text-chess-green">Drop your PGN file here...</p>
        ) : (
          <div>
            <p className="text-lg text-gray-300 mb-2">
              Drag & drop a PGN file here, or click to select
            </p>
            <p className="text-sm text-gray-500">
              Support for single games or entire databases
            </p>
          </div>
        )}
      </div>

      {selectedFile && (
        <div className="mt-6 bg-gray-800/50 rounded-lg p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <File className="w-8 h-8 text-chess-green" />
            <div>
              <p className="text-white font-medium">{selectedFile.name}</p>
              <p className="text-sm text-gray-400">
                {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
              </p>
            </div>
          </div>
          <button
            onClick={clearFile}
            className="p-2 hover:bg-gray-700 rounded-lg transition-colors"
            disabled={uploading}
          >
            <X className="w-5 h-5 text-gray-400" />
          </button>
        </div>
      )}

      {error && (
        <div className="mt-4 bg-red-500/10 border border-red-500/50 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <p className="text-red-400">{error}</p>
        </div>
      )}

      {selectedFile && !error && (
        <button
          onClick={handleUpload}
          disabled={uploading}
          className="mt-6 w-full bg-chess-green hover:bg-chess-green/80 disabled:bg-gray-700 disabled:text-gray-500 text-white font-semibold py-4 rounded-lg transition-all transform hover:scale-[1.02] active:scale-[0.98]"
        >
          {uploading ? 'Uploading...' : 'Upload and Process'}
        </button>
      )}
    </div>
  );
}
