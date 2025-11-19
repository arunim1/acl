'use client';

import Link from 'next/link';
import { Upload, BarChart3, TrendingDown, Zap, Database, Eye } from 'lucide-react';

export default function Home() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-900 via-gray-800 to-gray-900">
      {/* Hero Section */}
      <div className="container mx-auto px-4 py-20">
        <div className="text-center mb-16 animate-slide-in">
          <h1 className="text-6xl font-bold mb-6 bg-gradient-to-r from-chess-gold via-chess-green to-chess-beige bg-clip-text text-transparent">
            ChessInsights
          </h1>
          <p className="text-2xl text-gray-300 mb-4">
            Analyze Chess Game Quality Through Centipawn Loss
          </p>
          <p className="text-lg text-gray-400 max-w-2xl mx-auto mb-8">
            Explore massive Lichess databases with powerful visualizations.
            Discover patterns in move accuracy, time management, and player strength.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/analyze"
              className="inline-flex items-center gap-2 bg-chess-green hover:bg-chess-green/80 text-white px-8 py-4 rounded-lg text-lg font-semibold transition-all transform hover:scale-105 shadow-lg"
            >
              <Upload className="w-5 h-5" />
              Start Analyzing
            </Link>
            <a
              href="#features"
              className="inline-flex items-center gap-2 border-2 border-chess-green text-chess-green hover:bg-chess-green/10 px-8 py-4 rounded-lg text-lg font-semibold transition-all"
            >
              <Eye className="w-5 h-5" />
              View Features
            </a>
          </div>
        </div>

        {/* Features Grid */}
        <div id="features" className="grid md:grid-cols-2 lg:grid-cols-3 gap-8 mb-20">
          <FeatureCard
            icon={<Upload className="w-8 h-8" />}
            title="Easy Upload"
            description="Upload PGN files or stream directly from Lichess databases. Process thousands of games in seconds."
          />
          <FeatureCard
            icon={<BarChart3 className="w-8 h-8" />}
            title="Rich Visualizations"
            description="Interactive charts, heatmaps, and distributions. Explore data from every angle."
          />
          <FeatureCard
            icon={<TrendingDown className="w-8 h-8" />}
            title="Centipawn Loss Metrics"
            description="Understand move quality through precise centipawn loss analysis. Track accuracy trends."
          />
          <FeatureCard
            icon={<Zap className="w-8 h-8" />}
            title="Lightning Fast"
            description="Optimized processing pipeline handles millions of moves efficiently. Real-time progress updates."
          />
          <FeatureCard
            icon={<Database className="w-8 h-8" />}
            title="Powerful Filtering"
            description="Filter by ELO, time control, opening, date range, and more. Drill down to specific patterns."
          />
          <FeatureCard
            icon={<Eye className="w-8 h-8" />}
            title="Deep Insights"
            description="Auto-generated statistics reveal blunder rates, time pressure impact, and critical positions."
          />
        </div>

        {/* Stats Section */}
        <div className="bg-gray-800/50 rounded-2xl p-12 backdrop-blur-sm border border-gray-700">
          <div className="grid md:grid-cols-3 gap-8 text-center">
            <StatCard number="100M+" label="Moves Analyzed" />
            <StatCard number="<5s" label="Process 1K Games" />
            <StatCard number="10+" label="Chart Types" />
          </div>
        </div>

        {/* How It Works */}
        <div className="mt-20">
          <h2 className="text-4xl font-bold text-center mb-12 text-white">How It Works</h2>
          <div className="grid md:grid-cols-4 gap-6">
            <StepCard number="1" title="Upload Data" description="Choose PGN files or select Lichess database" />
            <StepCard number="2" title="Configure Filters" description="Set ELO range, time controls, and preferences" />
            <StepCard number="3" title="Process Games" description="Watch real-time progress as games are analyzed" />
            <StepCard number="4" title="Explore Insights" description="Interactive charts and detailed statistics" />
          </div>
        </div>
      </div>

      {/* Footer */}
      <footer className="border-t border-gray-800 mt-20 py-8">
        <div className="container mx-auto px-4 text-center text-gray-400">
          <p>Built with Next.js, TypeScript, and powered by Lichess data</p>
          <p className="mt-2 text-sm">Analyze chess games • Improve your play • Understand patterns</p>
        </div>
      </footer>
    </div>
  );
}

function FeatureCard({ icon, title, description }: { icon: React.ReactNode; title: string; description: string }) {
  return (
    <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-6 hover:border-chess-green/50 transition-all hover:transform hover:scale-105">
      <div className="text-chess-gold mb-4">{icon}</div>
      <h3 className="text-xl font-semibold text-white mb-2">{title}</h3>
      <p className="text-gray-400">{description}</p>
    </div>
  );
}

function StatCard({ number, label }: { number: string; label: string }) {
  return (
    <div>
      <div className="text-4xl font-bold text-chess-gold mb-2">{number}</div>
      <div className="text-gray-400">{label}</div>
    </div>
  );
}

function StepCard({ number, title, description }: { number: string; title: string; description: string }) {
  return (
    <div className="relative">
      <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-6 h-full">
        <div className="w-12 h-12 bg-chess-green rounded-full flex items-center justify-center text-white font-bold text-xl mb-4">
          {number}
        </div>
        <h3 className="text-lg font-semibold text-white mb-2">{title}</h3>
        <p className="text-gray-400 text-sm">{description}</p>
      </div>
    </div>
  );
}
