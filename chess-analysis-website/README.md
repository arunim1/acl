# ChessInsights - Centipawn Loss Analysis Platform

![ChessInsights](https://img.shields.io/badge/Next.js-16.0-black?logo=next.js)
![TypeScript](https://img.shields.io/badge/TypeScript-5.7-blue?logo=typescript)
![License](https://img.shields.io/badge/license-MIT-green)

**ChessInsights** is a comprehensive web platform for analyzing chess game quality through centipawn loss metrics. Built to handle massive Lichess databases with powerful visualizations and deep statistical insights.

## ✨ Features

### 🎯 Core Features

- **📤 Easy Upload**: Upload PGN files or stream directly from Lichess databases
- **📊 Rich Visualizations**: Interactive charts including:
  - Centipawn Loss vs Time Spent (scatter plot)
  - CPL Distribution (histogram)
  - Game Phase Analysis (bar chart)
  - Opening Analysis (horizontal bar chart)
- **🔍 Powerful Filtering**: Filter by ELO, time control, player color, game result
- **📈 Statistical Insights**: Auto-generated metrics including:
  - Average & Median Centipawn Loss
  - Accuracy Rate (% moves with CPL < 50)
  - Blunder Rate (% moves with CPL > 200)
  - Average Time per Move
- **💾 Data Export**: Export analyzed data as JSON or CSV
- **⚡ Lightning Fast**: Optimized processing handles thousands of games in seconds

### 🎨 Design Features

- **Dark Mode**: Chess-themed color palette optimized for visibility
- **Responsive Layout**: Works seamlessly on desktop, tablet, and mobile
- **Smooth Animations**: Delightful transitions and loading states
- **Accessibility**: WCAG AA compliant color contrasts

## 🚀 Quick Start

### Prerequisites

- Node.js 18+
- npm or yarn

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd chess-analysis-website

# Install dependencies
npm install

# Run development server
npm run dev

# Build for production
npm run build

# Start production server
npm start
```

Visit `http://localhost:3000` to see the application.

## 🏗️ Architecture

### Tech Stack

**Frontend:**
- Next.js 16 (App Router with Turbopack)
- TypeScript (strict mode)
- Tailwind CSS v4 with @tailwindcss/postcss
- Recharts for data visualization
- React Query for data fetching
- Zustand for state management

**Backend:**
- Next.js API Routes
- Python integration (chess_parser.py)
- File-based storage for uploads

**Data Processing:**
- Existing PGN streaming pipeline (main.py, chess_parser.py)
- Real-time processing with progress updates
- Efficient filtering and aggregation

### Project Structure

```
chess-analysis-website/
├── app/
│   ├── page.tsx                 # Landing page
│   ├── analyze/page.tsx         # Main analysis dashboard
│   ├── layout.tsx               # Root layout
│   ├── globals.css              # Global styles
│   ├── providers.tsx            # React Query provider
│   └── api/
│       ├── upload/route.ts      # File upload endpoint
│       ├── process/route.ts     # PGN processing endpoint
│       └── data/[sessionId]/route.ts  # Data retrieval
├── components/
│   ├── charts/                  # Chart components
│   │   ├── CPLvsTimeScatter.tsx
│   │   ├── CPLDistribution.tsx
│   │   ├── MovePhaseChart.tsx
│   │   └── OpeningAnalysis.tsx
│   ├── filters/                 # Filter components
│   │   ├── FilterPanel.tsx
│   │   └── AdvancedFilters.tsx
│   ├── insights/                # Statistics cards
│   │   └── InsightsDashboard.tsx
│   ├── upload/                  # Upload components
│   │   ├── FileUploader.tsx
│   │   └── ProcessingProgress.tsx
│   ├── export/                  # Export functionality
│   │   └── DataExporter.tsx
│   ├── ui/                      # UI primitives
│   │   ├── Button.tsx
│   │   ├── Card.tsx
│   │   └── Skeleton.tsx
│   └── Dashboard.tsx            # Main dashboard
├── lib/
│   ├── types.ts                 # TypeScript types
│   └── utils.ts                 # Utility functions
├── python-backend/              # Python processing
│   ├── chess_parser.py          # PGN parser
│   ├── config.py                # Configuration
│   └── processor.py             # Web processing wrapper
└── public/
    └── samples/                 # Sample PGN files
```

## 📖 Usage Guide

### Analyzing Your Games

1. **Upload PGN File**
   - Navigate to `/analyze`
   - Drag & drop a `.pgn` file or click to select
   - Click "Upload and Process"

2. **View Analysis**
   - Wait for processing to complete (usually < 5 seconds for 1000 games)
   - Explore interactive charts and statistics
   - Use filters to drill down into specific data

3. **Export Results**
   - Scroll to "Export Data" section
   - Choose JSON (for developers) or CSV (for spreadsheets)
   - Save the file for further analysis

### Understanding Metrics

**Centipawn Loss (CPL)**
- Measure of move accuracy in hundredths of a pawn
- Lower is better (0 = perfect move)
- <10: Excellent, 10-25: Good, 25-50: Inaccuracy, 50-100: Mistake, 100-200: Blunder, 200+: Huge Blunder

**Accuracy Rate**
- Percentage of moves with CPL < 50
- Higher is better
- Typical values: 70-85% for 1500-2000 ELO

**Blunder Rate**
- Percentage of moves with CPL > 200
- Lower is better
- Typical values: 2-5% for strong players

## 🔧 Configuration

### Environment Variables

Create a `.env.local` file:

```env
# Optional: Custom upload directory
UPLOAD_DIR=/tmp/chess-uploads

# Optional: Max file size (in bytes)
MAX_FILE_SIZE=104857600  # 100MB

# Optional: Processing timeout (in seconds)
PROCESSING_TIMEOUT=300
```

### Customizing Filters

Edit `config.py` in `python-backend/` to adjust:
- `min_elo`: Minimum ELO rating (default: 1500)
- `require_clock_and_eval`: Require timing and evaluation data
- `max_csv_buffer_size`: Buffer size for CSV writing

## 🎨 Customization

### Color Scheme

Edit `tailwind.config.ts` to customize colors:

```typescript
colors: {
  chess: {
    green: '#769656',   // Board green
    beige: '#EEEED2',   // Board beige
    dark: '#312E2B',    // Dark square
    gold: '#F0C040',    // Accent
  },
}
```

### Chart Appearance

Charts use Recharts. Customize in respective component files:
- `components/charts/CPLvsTimeScatter.tsx`
- `components/charts/CPLDistribution.tsx`
- etc.

## 🚢 Deployment

### Vercel (Recommended)

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel
```

Or connect your GitHub repository to Vercel for automatic deployments.

### Environment Setup

- Set `NODE_ENV=production`
- Configure upload directory with persistent storage
- Set appropriate memory limits for processing large files

### Performance Tips

1. **Optimize Uploads**: Use CDN for large file uploads
2. **Caching**: Consider Redis for processed data caching
3. **Database**: For large-scale deployments, use PostgreSQL or DuckDB
4. **Streaming**: Implement server-sent events for real-time progress

## 🧪 Testing

```bash
# Type checking
npm run type-check

# Linting
npm run lint

# Build verification
npm run build
```

## 📊 Data Format

### Input: PGN Format

Games must include:
- `[%eval X.XX]` - Engine evaluation
- `[%clk H:MM:SS]` - Clock time
- Standard metadata (ELO, TimeControl, Opening, etc.)

Example:
```
[Event "Rated Blitz game"]
[Site "https://lichess.org/abcd1234"]
[White "Player1"]
[Black "Player2"]
[WhiteElo "2000"]
[BlackElo "2100"]
[TimeControl "180+0"]
[ECO "B20"]
[Opening "Sicilian Defense"]

1. e4 { [%eval 0.09] [%clk 0:03:00] } c5 { [%eval 0.17] [%clk 0:02:59] }
...
```

### Output: JSON Format

```json
{
  "summary": {
    "totalGames": 100,
    "processedGames": 95,
    "rejectedGames": 5,
    "totalMoves": 4520,
    "timeControls": ["180+0", "300+0"],
    "eloRange": { "min": 1500, "max": 2500 }
  },
  "games": [
    {
      "gameId": "game_1",
      "whiteElo": 2000,
      "blackElo": 2100,
      "timeControl": "180+0",
      "openingEco": "B20",
      "openingName": "Sicilian Defense",
      "result": "1-0",
      "date": "2024-01-01",
      "moves": [
        {
          "moveNumber": "1w",
          "eval": 0.09,
          "centipawnLoss": 9,
          "timeLeft": 180,
          "timeSpent": 0,
          "color": "white"
        },
        ...
      ]
    },
    ...
  ]
}
```

## 🤝 Contributing

Contributions are welcome! Areas for improvement:

1. **Real-time Streaming**: Lichess API integration for live game streaming
2. **User Accounts**: Save analysis history and compare over time
3. **ML Insights**: Predict player strength, identify opening weaknesses
4. **Puzzle Generation**: Create training puzzles from high-CPL positions
5. **Social Features**: Share analysis, compare with friends
6. **Chess Board**: Interactive replay of analyzed games

## 📝 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

- **Lichess** for providing open chess databases
- **Recharts** for excellent charting library
- **Next.js** team for amazing framework
- Chess community for inspiration

## 📧 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Check existing documentation
- Review sample PGN files in `public/samples/`

---

**Built with ♟️ by chess enthusiasts, for chess enthusiasts**
