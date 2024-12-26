import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import os
import re
from math import ceil, sqrt

def extract_time_control(filename):
    """Extract base time and increment from filename."""
    match = re.search(r'_(\d+)\+(\d+)\.csv$', filename)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def aggregate_by_time(df):
    """Compute mean and std dev of centipawn loss for each second spent."""
    aggs = [
        pl.col("Centipawn Loss").mean().alias("mean_loss"),
        pl.col("Centipawn Loss").std().alias("std_loss"),
        pl.len().alias("n_moves")
    ]
    return df.group_by("Time Spent").agg(aggs).sort("Time Spent")

def create_plot(df, title, subplot, base_time):
    """Create a single plot with the given data."""
    # Aggregate data by time spent
    agg_df = aggregate_by_time(df)
    agg_df = agg_df.filter(pl.col("n_moves") >= 10)  # Only show points with enough data
    
    x_values = agg_df["Time Spent"].to_numpy()
    y_values = agg_df["mean_loss"].to_numpy()
    y_std = agg_df["std_loss"].to_numpy()
    
    max_time = base_time / 3
    
    # Plot mean line
    subplot.plot(x_values, y_values, '-', linewidth=1, alpha=0.8, label='Mean')
    
    # Plot standard deviation band
    subplot.fill_between(x_values, 
                        y_values - y_std,
                        y_values + y_std,
                        alpha=0.2,
                        label='±1 std dev')
    
    # subplot.set_ylim(0, 10)  # Max centipawn loss of 10
    subplot.set_xlim(0, max_time + 1)
    subplot.set_title(f'{title}\n(n={df.height:,} total moves)')
    subplot.set_xlabel('Time Spent (seconds)')
    subplot.set_ylabel('Centipawn Loss')

    # Linear scale with appropriate ticks
    x_ticks = np.linspace(0, max_time, 6)  # 6 ticks including 0
    y_ticks = np.linspace(0, 10, 6)  # 6 ticks including 0
    
    subplot.set_xticks(x_ticks)
    subplot.set_yticks(y_ticks)

    # Grid and legend
    subplot.grid(True, which='major', linestyle='-', alpha=0.3)
    subplot.legend(loc='upper right', fontsize='small')

def main(only_no_increment=True):
    # List CSV files in the output directory
    output_dir = 'output'
    files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    
    # Filter and sort files
    valid_files = []
    for f in files:
        base_time, increment = extract_time_control(f)
        if base_time is not None:
            if only_no_increment and increment == 0:
                valid_files.append((f, base_time, increment))
            elif not only_no_increment:
                valid_files.append((f, base_time, increment))
    
    # Sort by base time
    valid_files.sort(key=lambda x: x[1])
    
    if not valid_files:
        print("No valid CSV files found!")
        return

    # Read and process all dataframes
    dfs_dict = {}
    total_moves = 0
    MIN_MOVES = 10000  # Minimum number of moves required for plotting
    
    for fname, base_time, increment in valid_files:
        df = pl.read_csv(os.path.join(output_dir, fname), infer_schema_length=0)
        df = df.with_columns([
            pl.col("Move Number").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("Numeric Move"),
            pl.col("Move Number").str.slice(-1, 1).alias("Side"),
            pl.col("Centipawn Loss").cast(pl.Float64),
            pl.col("Time Spent").cast(pl.Float64)
        ])
        # Filter out high centipawn loss and time spent values
        df = df.filter(
            (pl.col("Centipawn Loss") <= 10) & 
            (pl.col("Time Spent") <= base_time / 5)
        )
        
        # Only include time controls with sufficient data
        if df.height >= MIN_MOVES:
            dfs_dict[f'{base_time}+{increment}'] = df
            total_moves += df.height
        else:
            print(f"Skipping {base_time}+{increment} (only {df.height:,} moves)")

    if not dfs_dict:
        print("No time controls with sufficient data found!")
        return

    # Calculate grid dimensions
    n = len(dfs_dict)
    cols = min(3, n)  # Maximum 3 columns
    rows = ceil(n / cols)
    
    # Create figure
    fig = plt.figure(figsize=(7*cols, 6*rows))
    fig.suptitle('Time Spent vs. Mean Centipawn Loss by Time Control' + 
                 (' (No Increment Games Only)' if only_no_increment else '') +
                 f'\nTotal moves analyzed: {total_moves:,}',
                 y=1.02)
    
    # Create subplots
    for i, ((title, df), (_, base_time, _)) in enumerate(zip(dfs_dict.items(), valid_files), 1):
        ax = fig.add_subplot(rows, cols, i)
        create_plot(df, title, ax, base_time)

    plt.tight_layout()
    output_name = 'time_vs_centipawn_loss_aggregated_no_increment.png' if only_no_increment else 'time_vs_centipawn_loss_aggregated_all.png'
    plt.savefig(output_name, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Plot has been generated and saved as {output_name}")

if __name__ == "__main__":
    main(only_no_increment=False)  # Set to False to include games with increments
