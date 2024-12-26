"""Efficient chess game parsing and processing."""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class GameState:
    """Tracks the state of a game being processed."""
    moves: List[str] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)
    has_clk_eval: bool = False
    reject: bool = False
    prev_white_clock: Optional[int] = None
    prev_black_clock: Optional[int] = None
    prev_eval: float = 0.0

class ChessParser:
    """Efficient parser for chess games with clock and evaluation data."""
    
    # Precompiled patterns
    MOVE_PATTERN = re.compile(
        r'(\d+)\.\s*([^\{]+)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}\s*'
        r'([^\{]*)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}'
    )
    
    # First character lookup for fast filtering
    METADATA_START = '['
    EVENT_START = '[Event '
    
    @staticmethod
    def clock_to_seconds(clock_str: str) -> int:
        """Convert clock time string (HH:MM:SS) to total seconds."""
        parts = clock_str.split(':')
        if len(parts) == 2:  # MM:SS format
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:  # HH:MM:SS format
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        return 0

    @staticmethod
    def parse_eval(eval_str: str) -> float:
        """Convert evaluation string to float, handling mate scores."""
        if eval_str.startswith('#'):
            return -10.0 if eval_str.startswith('#-') else 10.0
        return float(eval_str)

    @staticmethod
    def parse_time_control(tc_str: str) -> tuple[int, int]:
        """Parse time control string into initial time and increment."""
        parts = tc_str.split('+')
        return int(parts[0]), int(parts[1])

    def process_moves(self, state: GameState, csv_buffer: List[List], min_elo: int) -> None:
        """Process moves for a single game and append rows to CSV buffer."""
        if not state.moves or not state.has_clk_eval or state.reject:
            return

        # Get time control values
        initial_time, increment = self.parse_time_control(state.metadata.get('TimeControl', '0+0'))

        for move_line in state.moves:
            matches = list(self.MOVE_PATTERN.finditer(move_line))
            
            for match in matches:
                move_num, white_move, white_eval, white_clock, black_move, black_eval, black_clock = match.groups()
                
                # Process white's move
                white_clock_secs = self.clock_to_seconds(white_clock)
                white_time_spent = (initial_time - white_clock_secs if state.prev_white_clock is None 
                                  else state.prev_white_clock - white_clock_secs) + increment
                
                white_eval_float = self.parse_eval(white_eval)
                csv_buffer.append([
                    f"{move_num}w",
                    white_eval_float,
                    max(0, state.prev_eval - white_eval_float),
                    white_clock_secs,
                    white_time_spent
                ])
                state.prev_eval = white_eval_float
                
                # Process black's move if it exists
                if black_move:
                    black_clock_secs = self.clock_to_seconds(black_clock)
                    black_time_spent = (initial_time - black_clock_secs if state.prev_black_clock is None 
                                      else state.prev_black_clock - black_clock_secs) + increment
                    
                    black_eval_float = self.parse_eval(black_eval)
                    csv_buffer.append([
                        f"{move_num}b",
                        black_eval_float,
                        max(0, black_eval_float - state.prev_eval),
                        black_clock_secs,
                        black_time_spent
                    ])
                    state.prev_eval = black_eval_float
                    state.prev_black_clock = black_clock_secs
                
                state.prev_white_clock = white_clock_secs

    def should_reject_game(self, line: str, state: GameState, min_elo: int) -> bool:
        """Quickly determine if a game should be rejected based on metadata."""
        if line.startswith('[WhiteElo'):
            elo = int(line.split('"')[1])
            return elo < min_elo
        elif line.startswith('[BlackElo'):
            elo = int(line.split('"')[1])
            return elo < min_elo
        elif line.startswith('[Termination'):
            return 'Abandoned' in line
        return False
