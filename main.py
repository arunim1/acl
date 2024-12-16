import requests
import zstandard as zstd
import re
import csv
import os
import cProfile
import pstats
import io
import time

URL = "https://database.lichess.org/standard/lichess_db_standard_rated_2018-01.pgn.zst"

move_pattern = re.compile(
    r'(\d+)\.\s*([^\{]+)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}\s*'
    r'([^\{]*)\s*\{\s*\[%eval\s*([^\]]*)\]\s*\[%clk\s*([^\]]*)\]\s*\}'
)

def clock_to_seconds(clock_str):
    """Convert clock time string (HH:MM:SS) to total seconds."""
    parts = clock_str.split(':')
    if len(parts) == 2:  # MM:SS format
        return int(parts[0]) * 60 + int(parts[1])
    elif len(parts) == 3:  # HH:MM:SS format
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    return 0

def process_game_moves(moves, csv_buffer, metadata):
    """
    Process moves for a single game and append rows to a CSV buffer.
    
    Args:
        moves (list): List of move lines
        csv_buffer (list): Accumulated rows for CSV writing
        metadata (dict): Game metadata including time control
    """
    time_control = metadata.get('TimeControl', '180+2')
    time_parts = time_control.split('+')
    initial_time = int(time_parts[0])
    increment = int(time_parts[1])

    prev_white_clock = None
    prev_black_clock = None

    prev_eval = 0.0

    for move_line in moves:
        matches = list(move_pattern.finditer(move_line))
        
        for match in matches:
            move_number = match.group(1)
            white_move = match.group(2).strip()
            white_eval = match.group(3)
            white_clock = match.group(4)
            black_move = match.group(5).strip() if match.group(5) else ""
            black_eval = match.group(6) if match.group(6) else ""
            black_clock = match.group(7) if match.group(7) else ""
            
            # Convert clock times
            white_clock_seconds = clock_to_seconds(white_clock)
            black_clock_seconds = clock_to_seconds(black_clock) if black_clock else 0
            
            # Calculate time spent
            if prev_white_clock is None:
                # First move for white
                white_time_spent = initial_time - white_clock_seconds
            else:
                white_time_spent = prev_white_clock - white_clock_seconds
            # Include increment
            white_time_spent += increment

            if black_clock:
                if prev_black_clock is None:
                    # First black move
                    black_time_spent = initial_time - black_clock_seconds
                else:
                    black_time_spent = prev_black_clock - black_clock_seconds
                black_time_spent += increment
            else:
                black_time_spent = 0

            # Handle mate evaluations
            if white_eval.startswith('#'):
                white_eval = '10' if not white_eval.startswith('#-') else '-10'
            white_eval = float(white_eval)
            if black_eval and black_eval.startswith('#'):
                black_eval = '10' if not black_eval.startswith('#-') else '-10'
            if black_eval:
                black_eval = float(black_eval)

            # Append white move row
            csv_buffer.append([
                f"{move_number}w",
                white_eval,
                max(0, prev_eval - white_eval),
                white_clock_seconds,
                white_time_spent
            ])

            prev_eval = white_eval

            # Append black move row if exists
            if black_move:
                csv_buffer.append([
                    f"{move_number}b",
                    black_eval,
                    max(0, black_eval - prev_eval),
                    black_clock_seconds,
                    black_time_spent
                ])

                prev_eval = black_eval

            # Update prev clocks
            prev_white_clock = white_clock_seconds
            if black_clock:
                prev_black_clock = black_clock_seconds
            
def stream_decompress_and_process(url, output_csv):
    max_lines = 100000
    # Setup CSV file and writer
    with requests.get(url, stream=True) as response, open(output_csv, 'w', newline='') as csv_file:
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")

        csv_writer = csv.writer(csv_file)
        if os.stat(output_csv).st_size == 0:
            csv_writer.writerow(['Move Number', 'Eval', 'Centipawn Loss', 'Time Left', 'Time Spent'])

        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(response.raw)

        # We will read in larger chunks and then split into lines
        chunk_size = 1024 * 1024  # 1 MB chunks
        buffer = ''
        csv_buffer = []

        # State tracking variables
        game_moves = []
        game_has_clk_eval = False
        game_metadata = {}
        reject = False

        def process_finished_game():
            # Only process if not rejected and has clk+eval
            if game_moves and game_has_clk_eval and not reject:
                process_game_moves(game_moves, csv_buffer, game_metadata)

        broke = False

        while not broke:
            chunk = reader.read(chunk_size)
            if not chunk:
                break
            buffer += chunk.decode('utf-8', errors='replace')

            # Split lines from the buffer
            lines = buffer.split('\n')
            # Keep the incomplete line at the end in buffer
            buffer = lines.pop()

            for line in lines:
                line = line.strip()
                # Check for start of a new game
                if line.startswith('[Event '):
                    # Process finished game
                    process_finished_game()

                    # Reset for new game
                    game_moves = []
                    game_has_clk_eval = False
                    game_metadata = {}
                    reject = False
                    continue

                # Metadata lines
                if line.startswith('['):
                    if line.startswith('[TimeControl'):
                        game_metadata['TimeControl'] = line.split('"')[1]
                    elif line.startswith('[WhiteElo'):
                        white_elo = int(line.split('"')[1])
                        if white_elo < 2000:
                            reject = True
                        game_metadata['WhiteElo'] = white_elo
                    elif line.startswith('[BlackElo'):
                        black_elo = int(line.split('"')[1])
                        if black_elo < 2000:
                            reject = True
                        game_metadata['BlackElo'] = black_elo
                    elif line.startswith('[Termination'):
                        if 'Abandoned' in line:
                            reject = True
                    # Skip other metadata lines
                    continue

                if not line:
                    # Empty line (game separator or just spacing)
                    continue

                # Move lines with eval and clk
                if '%clk' in line and '%eval' in line:
                    game_has_clk_eval = True
                    game_moves.append(line)

                # Periodically write out CSV buffer to reduce memory usage
                if len(csv_buffer) > 500000:
                    csv_writer.writerows(csv_buffer)
                    csv_buffer.clear()
                
                # max_lines -= 1
                # if max_lines == 0:
                #     broke = True
                #     break 

        # End of file: process the last game
        process_finished_game()

        # Write any remaining rows
        if csv_buffer:
            csv_writer.writerows(csv_buffer)

if __name__ == '__main__':
    PROFILE = False

    if PROFILE:
        # Create a profiler
        profiler = cProfile.Profile()
        
        # Start profiling
        profiler.enable()
        
    start = time.time()
    # Run the main function
    stream_decompress_and_process(URL, 'lichess_moment.csv')

    print(f"Time taken: {time.time() - start}")
    
    if PROFILE:
        # Stop profiling
        profiler.disable()
        
        # Create a string buffer to capture stats
        s = io.StringIO()
        
        # Sort stats by cumulative time
        stats = pstats.Stats(profiler, stream=s).sort_stats('cumtime')
        
        # Print the top 20 most time-consuming functions
        stats.print_stats(20)
        
        # Save detailed stats to a file
        with open('profile_stats2.txt', 'w') as f:
            stats.print_stats(f)
        
        # Optional: create a graphical visualization
        try:
            import snakeviz
            stats.dump_stats('profile_stats2.prof')
            print("\nRun 'snakeviz profile_stats2.prof' to visualize the profile.")
        except ImportError:
            print("\nInstall 'snakeviz' for graphical profiling visualization.")
