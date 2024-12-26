import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import os

# list the csv files in the current directory
files = [f for f in os.listdir('.') if f.endswith('.csv')]
fname = files[0]
df = pl.read_csv(fname, infer_schema_length=0)

# Extract numeric move number and side using Polars' string extraction
df = df.with_columns([
    # Extracting numeric move number from "Move Number"
    pl.col("Move Number").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("Numeric Move"),
    # Extracting side from "Move Number" (last character)
    pl.col("Move Number").str.slice(-1, 1).alias("Side")
])

# without any moves with move number < 10
# df = df.filter(pl.col("Numeric Move") >= 10)

# Convert to pandas for plotting since seaborn expects pandas DataFrames
df_pandas = df.to_pandas()


# Time Spent vs. Centipawn Loss, 
# # Normalize Eval values for color mapping
# eval_values = df_pandas['Eval'].astype(float)  
x_values = df_pandas['Time Spent'].astype(float)
y_values = df_pandas['Centipawn Loss'].astype(float)

# %%
plt.figure(figsize=(10, 6))
scatter = plt.scatter(x_values, y_values, alpha=0.1, s=4)

plt.ylim(0, 200)
plt.xlim(-1, 1000)
plt.title('Time Spent vs. Centipawn Loss')
plt.xlabel('Time Spent (seconds)')
plt.ylabel('Centipawn Loss')

# Set symlog scale with clear linear thresholds
x_threshold = 30
y_threshold = 10
plt.xscale('symlog', linthresh=x_threshold)
plt.yscale('symlog', linthresh=y_threshold)

# Create linear spaced ticks within thresholds and log spaced outside
x_linear = np.arange(0, x_threshold, 5)  # Every 5s in linear region
x_log = np.arange(x_threshold, 1000, 100)
x_ticks = np.concatenate([x_linear, x_log])

y_linear = np.arange(0, y_threshold, 2)  # Every 2cp in linear region
y_log = np.arange(y_threshold, 200, 40)  # Log spacing after threshold
y_ticks = np.concatenate([y_linear, y_log])

plt.xticks(x_ticks)
plt.yticks(y_ticks)

# Single grid system
plt.grid(True, which='major', linestyle='-', alpha=0.3)

# Add vertical and horizontal lines at thresholds
plt.axvline(x=x_threshold, color='k', linestyle='--', alpha=0.5, ymax=0.45)
plt.axhline(y=y_threshold, color='k', linestyle='--', alpha=0.5, xmax=0.43)

plt.tight_layout()
plt.savefig('time_vs_centipawn_loss.png', dpi=300, bbox_inches='tight')
plt.close()

print("Plot has been generated and saved as a PNG file.")
