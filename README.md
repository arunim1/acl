# ACL - Average Centipawn Loss Analysis

Analysis of Lichess chess game data, examining the relationship between time spent and centipawn loss.

## Project Structure

```
.
├── main.py              # Main entry point for data processing
├── head.pgn            # Sample PGN data for testing
├── utils/              # Utility modules
│   ├── chess_parser.py # Chess game parsing logic
│   ├── config.py       # Configuration management
│   ├── plot.py         # Visualization generation
│   └── speed_test.py   # Performance testing
└── results/            # Output files
    ├── *.png           # Generated visualizations
    ├── profile_stats.prof  # Performance profiling data
    └── speed_test.txt  # Speed test results
```

## Usage

Run the main data processing pipeline:
```bash
python main.py
```

Generate visualizations from processed data:
```bash
python utils/plot.py
```

## Data Source

Processes Lichess game database (PGN format with clock and evaluation data).
