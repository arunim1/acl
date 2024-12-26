import requests
import zstandard as zstd
import csv
import os
import cProfile
import pstats
import io
import time
from typing import TextIO, BinaryIO

from config import ExperimentConfig
from chess_parser import ChessParser, GameState

def process_game_chunk(
    lines: list[str],
    state: GameState,
    parser: ChessParser,
    csv_buffer: list,
    config: ExperimentConfig
) -> tuple[GameState, int]:
    """Process a chunk of game lines and return the updated state and lines processed.
    
    Args:
        lines: List of lines to process
        state: Current game state
        parser: Chess parser instance
        csv_buffer: Buffer for CSV output
        config: Experiment configuration
        
    Returns:
        Tuple of (updated game state, number of lines processed)
    """
    lines_processed = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        first_char = line[0] if line else ''
        
        if first_char == '[':
            if line.startswith(parser.EVENT_START):
                # Process previous game
                parser.process_moves(state, csv_buffer, config.min_elo)
                # Reset state for new game
                state = GameState()
            elif config.require_clock_and_eval:
                # Quick reject based on metadata
                if parser.should_reject_game(line, state, config.min_elo):
                    state.reject = True
                elif line.startswith('[TimeControl'):
                    state.metadata['TimeControl'] = line.split('"')[1]
        else:
            # Move lines
            if '%clk' in line and '%eval' in line:
                state.has_clk_eval = True
                state.moves.append(line)
        
        # Test mode handling
        if config.test_mode:
            lines_processed += 1
            if lines_processed >= config.test_max_lines:
                break
                
    return state, lines_processed

def stream_decompress_and_process(config: ExperimentConfig) -> None:
    """Stream and process chess games from a compressed PGN file."""
    parser = ChessParser()

    pgn_name = config.pgn_url.split('/')[-1] # still includes .pgn.zst
    temp_name = pgn_name.split('.')[0]

    config.base_name = f"{temp_name}_{config.min_elo}.csv"
    
    with requests.get(config.pgn_url, stream=True) as response, \
         open(config.base_name, 'w', newline='') as csv_file:
        
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")

        # Initialize CSV writer
        csv_writer = csv.writer(csv_file)
        if os.stat(config.base_name).st_size == 0:
            csv_writer.writerow(config.csv_headers)

        # Setup decompression
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(response.raw)
        
        # Processing state
        buffer = ''
        csv_buffer = []
        state = GameState()
        total_lines_processed = 0
        
        # Main processing loop
        while True:
            chunk = reader.read(config.chunk_size)
            if not chunk:
                break
                
            buffer += chunk.decode('utf-8', errors='replace')
            lines = buffer.split('\n')
            buffer = lines.pop()  # Keep incomplete line

            # Process chunk of lines
            state, lines_processed = process_game_chunk(
                lines, state, parser, csv_buffer, config
            )
            total_lines_processed += lines_processed

            # Write buffer if it's large enough
            if len(csv_buffer) > config.max_csv_buffer_size:
                csv_writer.writerows(csv_buffer)
                csv_buffer.clear()

            if config.test_mode and total_lines_processed >= config.test_max_lines:
                break

        # Process final game and write remaining buffer
        parser.process_moves(state, csv_buffer, config.min_elo)
        if csv_buffer:
            csv_writer.writerows(csv_buffer)

def main(args):
    config = ExperimentConfig(
        test_max_lines=args.test_max_lines,
        enable_profiling=args.enable_profiling,
    )

    if config.enable_profiling:
        profiler = cProfile.Profile()
        profiler.enable()
        
    start = time.time()
    stream_decompress_and_process(config)
    time_taken = time.time() - start
    print(f"Time taken: {time_taken}")
    
    if config.enable_profiling:
        profiler.disable()
        s = io.StringIO()
        stats = pstats.Stats(profiler, stream=s).sort_stats('cumtime')
        stats.print_stats(20)
        
        # Save detailed stats
        stats.dump_stats(config.profile_output)
        print(f"\nProfile stats saved to {config.profile_output}")
        print("Run 'snakeviz profile_stats.prof' to visualize the profile.")

    return time_taken

if __name__ == '__main__':
    # parse command-line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Process chess games from a compressed PGN file.')
    parser.add_argument('--test-max-lines', type=int, default=ExperimentConfig().test_max_lines)
    parser.add_argument('--enable-profiling', action='store_true')

    args = parser.parse_args()
    main(args)
