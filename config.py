"""Configuration management for chess game analysis experiments."""

from dataclasses import dataclass
from typing import Optional

@dataclass
class ExperimentConfig:
    # Data source
    pgn_url: str = "https://database.lichess.org/standard/lichess_db_standard_rated_2018-01.pgn.zst"
    output_file: str = "lichess_moment.csv"
    chunk_size: int = 1024 * 1024  # 1MB chunks for streaming
    
    # Game filtering
    min_elo: int = 2000
    require_clock_and_eval: bool = True
    exclude_abandoned: bool = True
    
    # Processing
    max_csv_buffer_size: int = 5000
    test_mode: bool = True
    test_max_lines: Optional[int] = 300000
    
    # Performance monitoring
    enable_profiling: bool = True
    profile_output: str = "profile_stats.prof"
    
    # CSV Output
    csv_headers = ['Move Number', 'Eval', 'Centipawn Loss', 'Time Left', 'Time Spent']
