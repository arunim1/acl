# Centipawn Loss Analyzer - Web Application Plan

## Project Overview
Interactive web application for analyzing chess games focusing on centipawn loss metrics, built with Next.js 14 (App Router) and designed for Vercel deployment.

## Core Features

### 1. Data Input & Processing
- **Sample Data Browser**: Pre-loaded Lichess database samples by time control
- **File Upload**: Support for PGN file uploads (with streaming for large files)
- **URL Import**: Fetch PGN files from Lichess database URLs
- **Real-time Processing**: Stream and process games with progress indicators

### 2. Analysis Dashboard
- **Overview Stats**: Total games analyzed, average centipawn loss, move count
- **Time Control Comparison**: Side-by-side comparison of different time controls
- **Interactive Charts**:
  - Time spent vs centipawn loss (scatter + trend line)
  - Move number vs centipawn loss distribution
  - ELO rating impact on accuracy
  - Opening phase vs middlegame vs endgame accuracy
- **Heatmaps**: Visualize patterns across move numbers and time spent

### 3. Filtering & Segmentation
- **ELO Range**: Filter by minimum/maximum player rating
- **Time Control**: Select specific time controls or ranges
- **Move Phase**: Opening (1-15), Middlegame (16-40), Endgame (40+)
- **Player Color**: White/Black/Both
- **Game Outcome**: Win/Loss/Draw
- **Date Range**: Filter by game date

### 4. Individual Game Analysis
- **Game Viewer**: Interactive board with move navigation
- **Move-by-Move Analysis**: Show eval, time spent, centipawn loss per move
- **Critical Moments**: Highlight biggest mistakes/blunders
- **Comparison**: Compare player performance to database averages

### 5. Personal Tracking
- **Upload Personal Games**: Analyze your own games
- **Progress Over Time**: Track improvement in accuracy
- **Weaknesses**: Identify time pressure patterns, opening weaknesses
- **Benchmarking**: Compare against similar-rated players

### 6. Data Export
- **CSV Export**: Download processed data
- **Chart Export**: Save visualizations as PNG/SVG
- **Report Generation**: PDF summary reports
- **API Access**: Programmatic access to processed data

## Technical Architecture

### Frontend (Next.js 14)
```
app/
├── (dashboard)/
│   ├── page.tsx                 # Main analysis dashboard
│   ├── compare/page.tsx         # Time control comparison
│   ├── games/[id]/page.tsx      # Individual game viewer
│   └── upload/page.tsx          # Data upload interface
├── api/
│   ├── process/route.ts         # Process PGN data
│   ├── analyze/route.ts         # Analyze processed games
│   ├── export/route.ts          # Export functionality
│   └── samples/route.ts         # Pre-loaded sample data
├── layout.tsx
└── globals.css

components/
├── charts/
│   ├── TimeVsLossChart.tsx
│   ├── DistributionChart.tsx
│   ├── ComparisonChart.tsx
│   └── Heatmap.tsx
├── chess/
│   ├── ChessBoard.tsx
│   ├── MoveList.tsx
│   └── AnalysisPanel.tsx
├── filters/
│   ├── FilterPanel.tsx
│   └── FilterControls.tsx
└── ui/
    ├── Button.tsx
    ├── Card.tsx
    ├── Progress.tsx
    └── Stats.tsx

lib/
├── chess-parser.ts              # PGN parsing logic
├── analysis-engine.ts           # Centipawn loss calculations
├── data-processor.ts            # Stream processing
├── db.ts                        # IndexedDB for client-side storage
└── utils.ts                     # Helper functions
```

### Data Flow
1. **Input**: PGN file (upload/URL/sample)
2. **Stream Processing**: Parse games in chunks (Web Workers)
3. **Storage**: IndexedDB for processed games (client-side)
4. **Analysis**: On-demand computation with caching
5. **Visualization**: React components with Recharts/Plotly
6. **Export**: Generate files on-demand

### Technologies

**Core:**
- Next.js 14 (App Router)
- TypeScript
- React 18
- Tailwind CSS

**Data Processing:**
- Web Workers for background processing
- IndexedDB (via Dexie.js) for client storage
- Streaming API for large files

**Visualization:**
- Recharts (primary charts)
- React-Chessboard for board display
- Chess.js for validation
- Framer Motion for animations

**UI Components:**
- Radix UI primitives
- shadcn/ui components
- Lucide icons

**State Management:**
- Zustand for global state
- React Query for server state

**Performance:**
- Web Workers for heavy computation
- Virtual scrolling for large lists
- Code splitting & lazy loading
- Memoization & useMemo
- Debouncing & throttling

## Key Features for Chess Lovers

1. **Beautiful Board Visualization**: High-quality piece sets and themes
2. **Opening Explorer**: See common openings in dataset
3. **Pattern Recognition**: Identify tactical themes
4. **Time Trouble Analysis**: See how accuracy degrades with clock pressure
5. **Comparison Tools**: Compare yourself to GMs, IMs, etc.
6. **Learning Insights**: "You lose X centipawns on average in Y position type"

## Key Features for Data Lovers

1. **Statistical Rigor**: Confidence intervals, standard deviations
2. **Multiple Visualization Types**: Scatter, line, histogram, heatmap, box plots
3. **Correlation Analysis**: Time vs accuracy, ELO vs accuracy
4. **Regression Lines**: Trend lines with R² values
5. **Raw Data Access**: Export underlying datasets
6. **Customizable Charts**: Adjust axes, filters, aggregations
7. **SQL-like Filtering**: Complex queries on the data

## Performance Optimization Strategy

### Processing Efficiency
- Stream processing (never load entire file in memory)
- Web Workers (don't block UI)
- Incremental parsing (process as data arrives)
- Smart caching (IndexedDB for processed data)

### Rendering Efficiency
- Virtual scrolling for large lists
- Canvas rendering for heavy visualizations
- Memoization of expensive calculations
- Lazy loading of components
- Code splitting by route

### Bundle Optimization
- Tree shaking
- Dynamic imports
- Minimize dependencies
- Use lightweight alternatives where possible
- Optimize images and assets

### Data Transfer
- Compression (gzip/brotli)
- Efficient serialization
- Progressive loading
- CDN for static assets

## User Experience Flow

### First Visit
1. **Landing Page**: Beautiful hero with sample visualization
2. **Try Sample**: Click to analyze pre-loaded dataset
3. **See Results**: Interactive dashboard with insights
4. **Explore**: Filter, drill down, compare
5. **Upload Own**: Try with personal games

### Power User
1. **Upload Multiple Files**: Batch processing
2. **Save Filters**: Bookmark specific analyses
3. **Export Reports**: Generate PDF summaries
4. **API Access**: Integrate with tools

## Development Phases

### Phase 1: Core Infrastructure (Days 1-2)
- Setup Next.js project
- Build PGN parser in TypeScript
- Implement basic data structures
- Create sample dataset

### Phase 2: Basic Analysis (Days 3-4)
- Process PGN to structured data
- Calculate centipawn loss metrics
- Basic visualization components
- Simple dashboard layout

### Phase 3: Advanced Features (Days 5-6)
- Filtering system
- Multiple chart types
- Game viewer
- Comparison tools

### Phase 4: Polish & Optimization (Days 7-8)
- Performance optimization
- UI/UX refinement
- Mobile responsiveness
- Error handling

### Phase 5: Testing & Iteration (Days 9-14)
- 20+ iterations of user testing
- Feature enhancements
- Edge case handling
- Production optimization

## Success Metrics

### Performance Targets
- First Contentful Paint: < 1.5s
- Time to Interactive: < 3s
- Process 1000 games: < 5s
- Chart render: < 100ms
- Bundle size: < 200kb initial

### User Experience
- Intuitive first-use (no tutorial needed)
- Delightful animations
- Responsive on all devices
- Accessible (WCAG 2.1 AA)
- Fast perceived performance

### Data Accuracy
- 100% accurate parsing
- Consistent with Python analysis
- Validated against known datasets
- Edge cases handled gracefully

## Deployment Strategy

### Vercel Configuration
- Automatic deployments on push
- Preview deployments for PRs
- Edge Functions for API routes
- CDN for static assets
- Environment variables for config

### Monitoring
- Vercel Analytics for performance
- Error tracking (Sentry)
- Usage analytics (privacy-focused)
- User feedback collection

## Future Enhancements (Post-MVP)

1. **Multiplayer Comparison**: Compare multiple players side-by-side
2. **AI Insights**: ML-powered pattern detection
3. **Social Features**: Share analyses, leaderboards
4. **Mobile App**: React Native companion
5. **Backend Processing**: Optional server-side for huge datasets
6. **Real-time Updates**: Live game analysis
7. **Tournament Analysis**: Bulk tournament processing
8. **Opening Database**: Link to opening theory
9. **Puzzle Generation**: Create puzzles from critical positions
10. **Coach Mode**: Tools for chess coaches

## Implementation Notes

### Critical Decisions
- **Client-side processing**: Keep it free, no server costs, privacy-focused
- **IndexedDB storage**: Persist data between sessions
- **Progressive enhancement**: Works without JS for basic features
- **Mobile-first**: Design for mobile, enhance for desktop
- **Accessibility**: Screen reader support, keyboard navigation

### Risk Mitigation
- **Large files**: Streaming + Web Workers
- **Browser compatibility**: Polyfills for older browsers
- **Memory leaks**: Proper cleanup in useEffect
- **State consistency**: Single source of truth
- **Error boundaries**: Graceful degradation

### Testing Strategy
- Unit tests for parsing logic
- Integration tests for data flow
- E2E tests for critical paths
- Performance benchmarks
- Cross-browser testing
- Mobile device testing

## Conclusion

This application will be a powerful, beautiful, and performant tool for chess enthusiasts and data analysts to explore the relationship between thinking time and move quality. By focusing on client-side processing, we keep costs low, performance high, and data private while delivering an exceptional user experience.
