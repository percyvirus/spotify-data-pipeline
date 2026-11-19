import os

from pyspark.sql import SparkSession

# Configuration
LOCAL_DATA_PATH = "/Volumes/PERCYVIRUS/DATASETS/metadata_datasets/spotify-dev-dataset"

def main():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("SpotifyDataExploration") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    # Disable INFO logs to keep the console output clean
    spark.sparkContext.setLogLevel("WARN")

    # The most critical files for a dimensional model
    files_to_explore = [
        "spotify_clean_parquet/artists.parquet",
        "spotify_clean_parquet/albums.parquet",
        "spotify_clean_parquet/tracks.parquet",
        "spotify_clean_playlists_parquet/playlists.parquet",
        "spotify_clean_parquet/available_markets.parquet",
        "spotify_clean_parquet/track_artists.parquet",
        "spotify_clean_parquet/artist_albums.parquet",
        "spotify_clean_parquet/artist_genres.parquet",
        "spotify_clean_playlists_parquet/playlist_tracks.parquet",
        "spotify_clean_audio_features_parquet/track_audio_features.parquet",
        "spotify_clean_track_files_parquet/track_files.parquet",
        "spotify_clean_parquet/artist_images.parquet",
        "spotify_clean_parquet/album_images.parquet",
        "spotify_clean_playlists_parquet/playlist_images.parquet",
    ]

    for rel_path in files_to_explore:
        full_path = os.path.join(LOCAL_DATA_PATH, rel_path)
        
        if os.path.exists(full_path):
            print(f"\n{'-'*50}")
            print(f"Exploring: {rel_path}")
            print(f"{'-'*50}")
            
            # Read the parquet file
            df = spark.read.parquet(full_path)
            
            print(f"Total Rows: {df.count()}")
            print("\nSchema:")
            df.printSchema()
            
            print("\nSample Data (First 3 rows):")
            df.show(3, truncate=True)
        else:
            print(f"\nWarning: File not found at {full_path}")

    spark.stop()

if __name__ == "__main__":
    main()