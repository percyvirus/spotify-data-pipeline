import os

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from google.cloud import bigquery

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "spotify-data-pipeline-513809")
MART_DATASET = os.getenv("MART_DATASET", "spotify_data_marts")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/app/assets/audio_features_correlation_matrix.png")

FEATURES = [
    ("tempo", "Tempo (BPM)", (0, 250)),
    ("energy", "Energy", (0.0, 1.0)),
    ("valence", "Valence", (0.0, 1.0)),
    ("danceability", "Danceability", (0.0, 1.0)),
    ("acousticness", "Acousticness", (0.0, 1.0)),
    ("loudness", "Loudness (dB)", (-60.0, 0.0)),
    ("liveness", "Liveness", (0.0, 1.0)),
    ("instrumentalness", "Instrumentalness", (0.0, 1.0)),
    ("speechiness", "Speechiness", (0.0, 1.0)),
]


def fetch_audio_sample():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            tempo, energy, valence, danceability, acousticness,
            loudness, liveness, instrumentalness, speechiness
        FROM `{PROJECT_ID}.{MART_DATASET}.mart_audio_features_sample`
    """
    print("Fetching 250k audio feature samples from BigQuery...")
    return client.query(query).to_dataframe()


def plot_correlation_grid(df):
    n = len(FEATURES)
    fig, axes = plt.subplots(n, n, figsize=(18, 18), dpi=200)
    plt.subplots_adjust(wspace=0.08, hspace=0.08)

    # Define colormap filling empty (0-count) bins with the lowest viridis color
    viridis_cmap = plt.get_cmap("viridis").copy()
    viridis_cmap.set_bad(viridis_cmap(0))

    for i, (col_y, label_y, range_y) in enumerate(FEATURES):
        for j, (col_x, label_x, range_x) in enumerate(FEATURES):
            ax = axes[i, j]

            if i == j:
                # Diagonal: Histogram showing percentage distribution
                data = df[col_x].dropna()
                data = data[(data >= range_x[0]) & (data <= range_x[1])]
                weights = np.ones_like(data) * 100.0 / len(data)
                ax.hist(
                    data,
                    bins=20,
                    range=range_x,
                    weights=weights,
                    color="#7b52ab",
                    edgecolor="white",
                    linewidth=0.5,
                )
                ax.set_xlim(range_x)
                ax.set_facecolor("#f5f5f7")
            else:
                # Off-diagonal: 2D log-density heatmap (viridis with dark purple floor)
                valid = df[[col_x, col_y]].dropna()
                ax.set_facecolor(viridis_cmap(0))
                ax.hist2d(
                    valid[col_x],
                    valid[col_y],
                    bins=50,
                    range=[range_x, range_y],
                    cmap=viridis_cmap,
                    norm=mcolors.LogNorm(vmin=1),
                )
                ax.set_xlim(range_x)
                ax.set_ylim(range_y)

            # Clean ticks and axis labels on outer borders only
            if i < n - 1:
                ax.set_xticklabels([])
            else:
                ax.set_xlabel(label_x, fontsize=9, fontweight="bold")
                ax.tick_params(axis="x", labelsize=7)

            if j > 0:
                ax.set_yticklabels([])
            else:
                ax.set_ylabel(label_y, fontsize=9, fontweight="bold")
                ax.tick_params(axis="y", labelsize=7)

            for spine in ax.spines.values():
                spine.set_visible(False)

    fig.suptitle(
        "Audio Features Correlation Heatmaps (2D Log Density & Histograms)",
        fontsize=15,
        fontweight="bold",
        y=0.92,
    )
    plt.savefig(OUTPUT_PATH, bbox_inches="tight")
    print(f"Correlation heatmap saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    sample_df = fetch_audio_sample()
    plot_correlation_grid(sample_df)