"""
PGN File Processor - Adapted for web application use
Processes uploaded PGN files and returns structured JSON data
"""
import json
import io
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass, asdict
from chess_parser import ChessParser, GameState

@dataclass
class GameData:
    """Structured game data for JSON output"""
    game_id: str
    white_elo: int
    black_elo: int
    time_control: str
    opening_eco: str
    opening_name: str
    result: str
    date: str
    moves: List[Dict[str, Any]]

@dataclass
class ProcessingResult:
    """Result of processing a PGN file"""
    total_games: int
    processed_games: int
    rejected_games: int
    total_moves: int
    games: List[GameData]
    time_controls: List[str]
    elo_range: tuple[int, int]

class PGNProcessor:
    """Process PGN files for web application"""

    def __init__(self, min_elo: int = 1500, max_games: Optional[int] = None):
        self.min_elo = min_elo
        self.max_games = max_games
        self.parser = ChessParser()

    def process_pgn_content(self, content: str) -> ProcessingResult:
        """
        Process PGN content and return structured data

        Args:
            content: PGN file content as string

        Returns:
            ProcessingResult with all processed game data
        """
        lines = content.split('\n')
        games_data = []
        total_games = 0
        processed_games = 0
        rejected_games = 0
        total_moves = 0
        time_controls = set()
        min_elo_seen = float('inf')
        max_elo_seen = 0

        state = GameState(
            moves=[],
            metadata={},
            has_clk_eval=False,
            reject=False,
            prev_white_clock=None,
            prev_black_clock=None,
            prev_eval=0.0
        )

        game_counter = 0
        current_game_id = None

        for line in lines:
            line = line.rstrip('\n')

            # Track game boundaries
            if line.startswith('[Event '):
                if current_game_id and not state.reject and state.has_clk_eval:
                    # Process completed game
                    game_data = self._extract_game_data(state, current_game_id)
                    if game_data:
                        games_data.append(game_data)
                        processed_games += 1
                        total_moves += len(game_data.moves)
                        time_controls.add(game_data.time_control)

                        # Track ELO range
                        min_elo_seen = min(min_elo_seen, game_data.white_elo, game_data.black_elo)
                        max_elo_seen = max(max_elo_seen, game_data.white_elo, game_data.black_elo)

                elif current_game_id and state.reject:
                    rejected_games += 1

                # Reset state for new game
                total_games += 1
                game_counter += 1
                current_game_id = f"game_{game_counter}"
                state = GameState(
                    moves=[],
                    metadata={},
                    has_clk_eval=False,
                    reject=False,
                    prev_white_clock=None,
                    prev_black_clock=None,
                    prev_eval=0.0
                )

                # Check if we've hit the limit
                if self.max_games and processed_games >= self.max_games:
                    break

            # Parse metadata
            if line.startswith('['):
                key_end = line.find(' ', 1)
                if key_end != -1:
                    key = line[1:key_end]
                    value = line[key_end+2:-2]  # Remove quotes and ]
                    state.metadata[key] = value

                    # Check ELO filtering
                    if key == 'WhiteElo' or key == 'BlackElo':
                        try:
                            elo = int(value)
                            if elo < self.min_elo:
                                state.reject = True
                        except ValueError:
                            state.reject = True

            # Parse moves
            elif line and not line.startswith('[') and not state.reject:
                # Check for eval and clk annotations
                if '[%eval' in line and '[%clk' in line:
                    state.has_clk_eval = True
                    state.moves.append(line)

        # Process final game
        if current_game_id and not state.reject and state.has_clk_eval:
            game_data = self._extract_game_data(state, current_game_id)
            if game_data:
                games_data.append(game_data)
                processed_games += 1
                total_moves += len(game_data.moves)
                time_controls.add(game_data.time_control)
                min_elo_seen = min(min_elo_seen, game_data.white_elo, game_data.black_elo)
                max_elo_seen = max(max_elo_seen, game_data.white_elo, game_data.black_elo)
        elif current_game_id and state.reject:
            rejected_games += 1

        return ProcessingResult(
            total_games=total_games,
            processed_games=processed_games,
            rejected_games=rejected_games,
            total_moves=total_moves,
            games=games_data,
            time_controls=sorted(list(time_controls)),
            elo_range=(int(min_elo_seen) if min_elo_seen != float('inf') else 0, int(max_elo_seen))
        )

    def _extract_game_data(self, state: GameState, game_id: str) -> Optional[GameData]:
        """Extract structured data from a game state"""
        try:
            # Process moves to get centipawn loss data
            moves_data = []
            csv_buffer = []

            # Use existing parser logic
            self.parser.process_moves(state, csv_buffer)

            # Convert CSV rows to structured move data
            for row in csv_buffer:
                move_num, eval_val, cpl, time_left, time_spent = row
                moves_data.append({
                    'moveNumber': move_num,
                    'eval': float(eval_val),
                    'centipawnLoss': float(cpl),
                    'timeLeft': int(time_left),
                    'timeSpent': int(time_spent),
                    'color': 'white' if str(move_num).endswith('w') else 'black'
                })

            # Extract metadata
            white_elo = int(state.metadata.get('WhiteElo', 0))
            black_elo = int(state.metadata.get('BlackElo', 0))
            time_control = state.metadata.get('TimeControl', 'unknown')
            opening_eco = state.metadata.get('ECO', 'unknown')
            opening_name = state.metadata.get('Opening', 'unknown')
            result = state.metadata.get('Result', '*')
            date = state.metadata.get('UTCDate', 'unknown')

            return GameData(
                game_id=game_id,
                white_elo=white_elo,
                black_elo=black_elo,
                time_control=time_control,
                opening_eco=opening_eco,
                opening_name=opening_name,
                result=result,
                date=date,
                moves=moves_data
            )
        except Exception as e:
            print(f"Error extracting game data: {e}")
            return None

    def process_to_json(self, content: str) -> str:
        """Process PGN and return JSON string"""
        result = self.process_pgn_content(content)

        # Convert to JSON-serializable format
        output = {
            'summary': {
                'totalGames': result.total_games,
                'processedGames': result.processed_games,
                'rejectedGames': result.rejected_games,
                'totalMoves': result.total_moves,
                'timeControls': result.time_controls,
                'eloRange': {
                    'min': result.elo_range[0],
                    'max': result.elo_range[1]
                }
            },
            'games': [asdict(game) for game in result.games]
        }

        return json.dumps(output, indent=2)

if __name__ == '__main__':
    # Test with sample data
    import sys

    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            content = f.read()

        processor = PGNProcessor(min_elo=1500)
        result = processor.process_to_json(content)
        print(result)
    else:
        print("Usage: python processor.py <pgn_file>")
