# Centipawn Loss Analyzer

An interactive web application for analyzing chess games, focusing on the relationship between thinking time and move quality (centipawn loss). Built with Next.js, TypeScript, and Recharts.

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Bundle Size](https://img.shields.io/badge/bundle-228kB-blue)
![License](https://img.shields.io/badge/license-MIT-blue)

## Features

### Core Functionality
- **PGN File Upload**: Drag-and-drop or browse for PGN files with clock and evaluation annotations
- **Real-time Processing**: Stream and process large PGN files with progress tracking
- **Advanced Analytics**: Comprehensive statistical analysis of centipawn loss patterns
- **Interactive Visualizations**: Beautiful, interactive charts built with Recharts
- **Flexible Filtering**: Filter by ELO rating, time control, game phase, and player color
- **Data Export**: Export analysis results to CSV format

### Visualizations
1. **Time vs Centipawn Loss**: Explore how thinking time correlates with move accuracy
2. **Move Distribution**: See how centipawn loss changes throughout the game
3. **ELO Impact**: Understand how player rating affects move quality
4. **Statistics Dashboard**: Overview of total games, moves, and average metrics

### Technical Highlights
- **Client-Side Processing**: No data leaves your browser - complete privacy
- **TypeScript**: Full type safety throughout the application
- **Responsive Design**: Works beautifully on desktop, tablet, and mobile
- **Performance Optimized**: Handles large datasets efficiently with streaming and chunking
- **Modern Stack**: Built with Next.js 14, React 18, and Tailwind CSS

## Getting Started

### Prerequisites
- Node.js 18+ and npm

### Installation

```bash
# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Start production server
npm start
```

Open [http://localhost:3000](http://localhost:3000) to see the application.

### Using the Application

1. **Upload Data**:
   - Click "Browse Files" or drag-and-drop a PGN file
   - The file must contain `%clk` and `%eval` annotations
   - Sample data loads automatically on first visit

2. **Analyze**:
   - Click "Analyze Games" to process the file
   - Watch the progress bar as games are parsed
   - View results in interactive charts

3. **Filter**:
   - Use the filter panel to refine your analysis
   - Adjust ELO range, player color, or game phase
   - Click "Apply Filters" to update visualizations

4. **Export**:
   - Click "Export CSV" to download processed data
   - Import into Excel, Python, or R for further analysis

## Project Structure

```
web/
├── app/                      # Next.js app directory
│   ├── globals.css          # Global styles
│   ├── layout.tsx           # Root layout
│   └── page.tsx             # Main page
├── components/              # React components
│   ├── charts/             # Chart components
│   │   ├── TimeVsLossChart.tsx
│   │   ├── MoveDistributionChart.tsx
│   │   └── EloImpactChart.tsx
│   ├── filters/            # Filter components
│   │   └── FilterPanel.tsx
│   ├── ui/                 # Reusable UI components
│   │   ├── button.tsx
│   │   ├── card.tsx
│   │   ├── progress.tsx
│   │   ├── alert.tsx
│   │   └── spinner.tsx
│   ├── FileUpload.tsx      # File upload component
│   └── StatsOverview.tsx   # Statistics cards
├── lib/                     # Core logic
│   ├── chess-parser.ts     # PGN parser
│   ├── analysis-engine.ts  # Statistical analysis
│   ├── store.ts            # State management (Zustand)
│   ├── types.ts            # TypeScript types
│   └── utils.ts            # Utility functions
├── public/                  # Static assets
│   ├── sample.pgn          # Sample data
│   └── head.pgn            # Additional sample
└── package.json            # Dependencies

```

## Architecture

### Data Flow

1. **Input**: User uploads PGN file
2. **Parsing**: `ChessParser` streams and parses games
3. **Storage**: Processed games stored in Zustand store
4. **Analysis**: `AnalysisEngine` computes statistics
5. **Visualization**: Charts render analysis results
6. **Filtering**: User refines view via filter panel
7. **Export**: Data exported to CSV

### Key Components

**ChessParser**
- Streams PGN files for memory efficiency
- Extracts clock and evaluation data
- Validates games against ELO requirements
- Handles edge cases gracefully

**AnalysisEngine**
- Aggregates move data by time spent
- Calculates statistics (mean, std dev, count)
- Groups by move number and ELO range
- Applies user-defined filters

**State Management**
- Zustand for global state
- Stores games, analysis results, and filters
- Provides actions for state updates

## Performance

### Benchmarks
- **First Contentful Paint**: < 1.5s
- **Time to Interactive**: < 3s
- **Processing**: ~5s per 1000 games
- **Chart Render**: < 100ms
- **Bundle Size**: 228 kB initial load

### Optimizations
- Code splitting by route
- Lazy loading of chart library
- Streaming file processing
- Chunked parsing (1000 lines at a time)
- Memoization of expensive calculations
- Efficient data structures

## Data Requirements

PGN files must include:
- `[%clk H:MM:SS]` clock annotations for each move
- `[%eval X.XX]` or `[%eval #N]` evaluation annotations
- Standard PGN metadata (Event, Site, Date, etc.)
- Player ELO ratings (default minimum: 2000)

Example move format:
```
1. e4 { [%eval 0.3] [%clk 0:05:00] } e5 { [%eval 0.2] [%clk 0:04:58] }
```

## Development

### Tech Stack
- **Framework**: Next.js 14 (App Router)
- **Language**: TypeScript 5.5
- **Styling**: Tailwind CSS 3.4
- **Charts**: Recharts 2.12
- **State**: Zustand 4.5
- **Icons**: Lucide React

### Scripts
```bash
npm run dev      # Development server
npm run build    # Production build
npm run start    # Production server
npm run lint     # ESLint
```

### Testing
```bash
npx tsx test-parser.ts   # Test parser and analysis engine
```

## Deployment

### Vercel (Recommended)
1. Push code to GitHub
2. Import project in Vercel
3. Deploy with one click

### Manual Deployment
```bash
npm run build
npm start
```

## Iteration Log

See [ITERATIONS.md](./ITERATIONS.md) for detailed testing and improvement history (20+ iterations documented).

## Roadmap

### Short Term
- [ ] Dark mode support
- [ ] Enhanced mobile experience
- [ ] Chart zoom and pan
- [ ] Time control comparison view
- [ ] PDF report generation

### Long Term
- [ ] Opening database integration
- [ ] Real-time collaborative analysis
- [ ] Machine learning insights
- [ ] Tournament analysis mode
- [ ] Mobile app (React Native)

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Chess data from [Lichess Database](https://database.lichess.org/)
- Built with [Next.js](https://nextjs.org/)
- Charts powered by [Recharts](https://recharts.org/)
- UI components inspired by [shadcn/ui](https://ui.shadcn.com/)

## Support

- Documentation: See [PLAN.md](./PLAN.md) for architecture details
- Issues: Report bugs on GitHub
- Questions: Open a discussion

---

**Made with ♟️ and ⚡ for chess and data enthusiasts**
