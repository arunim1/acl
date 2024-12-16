import polars as pl
import matplotlib.pyplot as plt

# ================================
# Step 1: Load the Data with Polars
# ================================
# Replace 'your_data.csv' with the actual path to your CSV file
df = pl.read_csv("./lichess_db_standard_2000rated_2018-01.csv", infer_schema_length=0)

# ================================
# Step 2: Data Transformation with Polars
# ================================
# Extract numeric move number and side using Polars' string extraction
df = df.with_columns([
    # Extracting numeric move number from "Move Number"
    pl.col("Move Number").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("Numeric Move"),
    # Extracting side from "Move Number" (last character)
    pl.col("Move Number").str.slice(-1, 1).alias("Side")
])

# Selecting numeric columns
numeric_cols = ["Eval", "Centipawn Loss", "Time Left", "Time Spent", "Numeric Move"]
df_numeric = df.select(numeric_cols)

# Convert to pandas for plotting since seaborn expects pandas DataFrames
df_pandas = df.to_pandas()
df_numeric_pandas = df_numeric.to_pandas()

# ================================
# Time Spent vs. Centipawn Loss, colored by Eval
# ================================
# Normalize Eval values for color mapping
eval_values = df_pandas['Eval'].astype(float)  # Convert to float
x_values = df_pandas['Time Spent'].astype(float)
y_values = df_pandas['Centipawn Loss'].astype(float)

# %%
plt.figure(figsize=(10, 6))
scatter = plt.scatter(x_values, y_values, 
                     c=eval_values, cmap='viridis', alpha=0.1, s=4)
plt.ylim(0, 200)
plt.xlim(-0.1, 1000)  # Start from 0.1 seconds
plt.colorbar(scatter, label='Eval')
plt.title('Time Spent vs. Centipawn Loss, colored by Eval')
plt.xlabel('Time Spent (seconds)')
plt.ylabel('Centipawn Loss')
plt.xscale('symlog', linthresh=1)  # Symmetric log scale with linear region below 1 second
plt.grid(True, alpha=0.3)
# plt.tight_layout()
plt.savefig('time_spent_centipawn_loss.png', dpi=300, bbox_inches='tight')
plt.close()

print("Plots have been generated and saved as PNG files.")
