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
    csv_buffers: dict,
    config: ExperimentConfig
) -> tuple[GameState, int]:
    """Process a chunk of game lines and return the updated state and lines processed.
    
    Args:
        lines: List of lines to process
        state: Current game state
        parser: Chess parser instance
        csv_buffers: Dictionary mapping time controls to their CSV buffers
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
                # Process previous game if we have a valid time control
                if state.metadata.get('TimeControl'):
                    time_control = state.metadata['TimeControl']
                    if time_control not in csv_buffers:
                        csv_buffers[time_control] = []
                    parser.process_moves(state, csv_buffers[time_control], config.min_elo)
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
    config.base_name = temp_name

    # Create output directory if it doesn't exist
    os.makedirs(config.output_dir, exist_ok=True)
    
    # Dictionary to store CSV writers and files for each time control
    csv_files = {}
    csv_writers = {}
    csv_buffers = {}
    
    with requests.get(config.pgn_url, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")

        # Setup decompression
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(response.raw)
        
        # Processing state
        buffer = ''  # Initialize as string, not list
        state = GameState()
        total_lines_processed = 0
        
        # Main processing loop
        while True:
            chunk = reader.read(config.chunk_size)
            if not chunk:
                # Process final game if needed
                if state.metadata.get('TimeControl'):
                    time_control = state.metadata['TimeControl']
                    if time_control not in csv_buffers:
                        csv_buffers[time_control] = []
                    parser.process_moves(state, csv_buffers[time_control], config.min_elo)
                break

            buffer += chunk.decode('utf-8')
            lines = buffer.split('\n')
            
            # Keep last partial line in buffer
            buffer = lines[-1]
            lines = lines[:-1]
            
            # Process chunk
            state, lines_processed = process_game_chunk(lines, state, parser, csv_buffers, config)
            total_lines_processed += lines_processed

            # Write buffers to files when they get too large
            for tc, buf in csv_buffers.items():
                if len(buf) >= config.max_csv_buffer_size:
                    if tc not in csv_files:
                        # Create new file for this time control
                        filename = f"{config.output_dir}/{config.base_name}_{tc}.csv"
                        csv_files[tc] = open(filename, 'w', newline='')
                        csv_writers[tc] = csv.writer(csv_files[tc])
                        # Write headers for new file
                        csv_writers[tc].writerow(config.csv_headers)
                    
                    # Write buffer to file
                    csv_writers[tc].writerows(buf)
                    csv_buffers[tc] = []

            if config.test_mode and total_lines_processed >= config.test_max_lines:
                break

        # Write remaining buffers and close files
        for time_control, buffer in csv_buffers.items():
            if buffer:
                if time_control not in csv_files:
                    filename = f"{config.output_dir}/{config.base_name}_{time_control}.csv"
                    csv_files[time_control] = open(filename, 'w', newline='')
                    csv_writers[time_control] = csv.writer(csv_files[time_control])
                    csv_writers[time_control].writerow(config.csv_headers)
                csv_writers[time_control].writerows(buffer)

        # Close all files
        for file in csv_files.values():
            file.close()

def main(args):
    config = ExperimentConfig(
        test_max_lines=args.test_max_lines,
        enable_profiling=args.enable_profiling,
        test_mode=not args.not_test_mode,
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
    parser.add_argument('--not-test-mode', action='store_true')

    args = parser.parse_args()
    main(args)
