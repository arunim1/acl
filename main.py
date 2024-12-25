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

def stream_decompress_and_process(config: ExperimentConfig) -> None:
    """Stream and process chess games from a compressed PGN file."""
    parser = ChessParser()
    
    with requests.get(config.pgn_url, stream=True) as response, \
         open(config.output_file, 'w', newline='') as csv_file:
        
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")

        # Initialize CSV writer
        csv_writer = csv.writer(csv_file)
        if os.stat(config.output_file).st_size == 0:
            csv_writer.writerow(config.csv_headers)

        # Setup decompression
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(response.raw)
        
        # Processing state
        buffer = ''
        csv_buffer = []
        state = GameState()
        lines_processed = 0
        
        def process_finished_game():
            parser.process_moves(state, csv_buffer, config.min_elo)
            
            # Write buffer if it's large enough
            if len(csv_buffer) > config.max_csv_buffer_size:
                csv_writer.writerows(csv_buffer)
                csv_buffer.clear()

        # Main processing loop
        while True:
            chunk = reader.read(config.chunk_size)
            if not chunk:
                break
                
            buffer += chunk.decode('utf-8', errors='replace')
            lines = buffer.split('\n')
            buffer = lines.pop()  # Keep incomplete line

            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                first_char = line[0] if line else ''
                
                if first_char == '[':
                    if line.startswith(parser.EVENT_START):
                        # Process previous game
                        process_finished_game()
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

            if config.test_mode and lines_processed >= config.test_max_lines:
                break

        # Process final game and write remaining buffer
        process_finished_game()
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
