import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuration
SOURCE_DATA_PATH = "/Volumes/PERCYVIRUS/DATASETS/metadata_datasets/spotify-metadata-by-annas-archive"
DEV_DATA_PATH = "/Volumes/PERCYVIRUS/DATASETS/metadata_datasets/spotify-dev-dataset"
POPULARITY_THRESHOLD = 30

def create_directory_structure():
    """Ensure the target directory exists and is clean."""
    if os.path.exists(DEV_DATA_PATH):
        print(f"Cleaning existing directory: {DEV_DATA_PATH}")
        shutil.rmtree(DEV_DATA_PATH)
    
    os.makedirs(DEV_DATA_PATH)
    os.makedirs(os.path.join(DEV_DATA_PATH, "spotify_clean_parquet"))
    os.makedirs(os.path.join(DEV_DATA_PATH, "spotify_clean_playlists_parquet"))
    os.makedirs(os.path.join(DEV_DATA_PATH, "spotify_clean_audio_features_parquet"))
    os.makedirs(os.path.join(DEV_DATA_PATH, "spotify_clean_track_files_parquet"))
    print("Created directory structure for Dev Dataset.")

def copy_json_files():
    """Copy small metadata JSON files directly."""
    src = os.path.join(SOURCE_DATA_PATH, "spotify_artist_redirects.json")
    dst = os.path.join(DEV_DATA_PATH, "spotify_artist_redirects.json")
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print("Copied spotify_artist_redirects.json")

def write_single_parquet(df, final_file_path):
    """
    Forces Spark to write a single parquet file instead of a directory of parts.
    It writes to a temp directory, moves the part-0000 file out, renames it, and cleans up.
    """
    temp_dir = final_file_path + "_temp"
    
    # 1. Coalesce to 1 partition and write to temp directory
    df.coalesce(1).write.mode("overwrite").parquet(temp_dir)
    
    # 2. Find the actual parquet part file inside the temp directory
    part_file = glob.glob(os.path.join(temp_dir, "part-*.parquet"))[0]
    
    # 3. Move it to the final destination with the correct name
    if os.path.exists(final_file_path):
        os.remove(final_file_path)
    shutil.move(part_file, final_file_path)
    
    # 4. Clean up the temporary Spark directory (_SUCCESS, .crc, etc.)
    shutil.rmtree(temp_dir)
    print(f"  -> Saved single file: {os.path.basename(final_file_path)}")

def main():
    print(f"Generating Dev Dataset (Popularity >= {POPULARITY_THRESHOLD})...")
    create_directory_structure()
    copy_json_files()

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("SpotifyDevDatasetCreator") \
        .master("local[*]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_src = os.path.join(SOURCE_DATA_PATH, "spotify_clean_parquet")
    base_dst = os.path.join(DEV_DATA_PATH, "spotify_clean_parquet")

    # --- 1. FILTER CORE ROOT (TRACKS) ---
    print(f"Filtering tracks (popularity >= {POPULARITY_THRESHOLD})...")
    df_all_tracks = spark.read.parquet(os.path.join(base_src, "tracks.parquet"))
    df_dev_tracks = df_all_tracks.filter(col("popularity") >= POPULARITY_THRESHOLD).cache()
    
    print(f"Found {df_dev_tracks.count()} popular tracks.")
    write_single_parquet(df_dev_tracks, os.path.join(base_dst, "tracks.parquet"))

    dev_track_rowids = df_dev_tracks.select(col("rowid").alias("dev_track_rowid")).distinct()
    dev_album_rowids = df_dev_tracks.select(col("album_rowid").alias("dev_album_rowid")).distinct()

    # --- 2. FILTER ALBUMS & ALBUM SATELLITES ---
    print("Filtering albums...")
    df_all_albums = spark.read.parquet(os.path.join(base_src, "albums.parquet"))
    df_dev_albums = df_all_albums.join(dev_album_rowids, df_all_albums.rowid == dev_album_rowids.dev_album_rowid, "inner").drop("dev_album_rowid")
    write_single_parquet(df_dev_albums, os.path.join(base_dst, "albums.parquet"))

    print("Filtering album images...")
    df_all_album_images = spark.read.parquet(os.path.join(base_src, "album_images.parquet"))
    df_dev_album_images = df_all_album_images.join(dev_album_rowids, df_all_album_images.album_rowid == dev_album_rowids.dev_album_rowid, "inner").drop("dev_album_rowid")
    write_single_parquet(df_dev_album_images, os.path.join(base_dst, "album_images.parquet"))

    # --- 3. FILTER ARTISTS & SATELLITES ---
    print("Filtering track_artists bridge...")
    df_all_track_artists = spark.read.parquet(os.path.join(base_src, "track_artists.parquet"))
    df_dev_track_artists = df_all_track_artists.join(dev_track_rowids, df_all_track_artists.track_rowid == dev_track_rowids.dev_track_rowid, "inner").drop("dev_track_rowid")
    write_single_parquet(df_dev_track_artists, os.path.join(base_dst, "track_artists.parquet"))

    dev_artist_rowids = df_dev_track_artists.select(col("artist_rowid").alias("dev_artist_rowid")).distinct()

    print("Filtering artists...")
    df_all_artists = spark.read.parquet(os.path.join(base_src, "artists.parquet"))
    df_dev_artists = df_all_artists.join(dev_artist_rowids, df_all_artists.rowid == dev_artist_rowids.dev_artist_rowid, "inner").drop("dev_artist_rowid")
    write_single_parquet(df_dev_artists, os.path.join(base_dst, "artists.parquet"))

    print("Filtering artist images, genres, and artist_albums...")
    df_all_artist_images = spark.read.parquet(os.path.join(base_src, "artist_images.parquet"))
    write_single_parquet(
        df_all_artist_images.join(dev_artist_rowids, df_all_artist_images.artist_rowid == dev_artist_rowids.dev_artist_rowid, "inner").drop("dev_artist_rowid"),
        os.path.join(base_dst, "artist_images.parquet")
    )

    df_all_artist_genres = spark.read.parquet(os.path.join(base_src, "artist_genres.parquet"))
    write_single_parquet(
        df_all_artist_genres.join(dev_artist_rowids, df_all_artist_genres.artist_rowid == dev_artist_rowids.dev_artist_rowid, "inner").drop("dev_artist_rowid"),
        os.path.join(base_dst, "artist_genres.parquet")
    )

    df_all_artist_albums = spark.read.parquet(os.path.join(base_src, "artist_albums.parquet"))
    write_single_parquet(
        df_all_artist_albums.join(dev_artist_rowids, df_all_artist_albums.artist_rowid == dev_artist_rowids.dev_artist_rowid, "inner").drop("dev_artist_rowid"),
        os.path.join(base_dst, "artist_albums.parquet")
    )

    # --- 4. FILTER FACTS (AUDIO FEATURES & TRACK FILES) ---
    print("Filtering audio features and track files...")
    dev_track_string_ids = df_dev_tracks.select(col("id").alias("dev_string_id")).distinct()
    
    audio_src = os.path.join(SOURCE_DATA_PATH, "spotify_clean_audio_features_parquet", "track_audio_features.parquet")
    audio_dst = os.path.join(DEV_DATA_PATH, "spotify_clean_audio_features_parquet", "track_audio_features.parquet")
    df_all_audio = spark.read.parquet(audio_src)
    write_single_parquet(
        df_all_audio.join(dev_track_string_ids, df_all_audio.track_id == dev_track_string_ids.dev_string_id, "inner").drop("dev_string_id"),
        audio_dst
    )

    files_src = os.path.join(SOURCE_DATA_PATH, "spotify_clean_track_files_parquet", "track_files.parquet")
    files_dst = os.path.join(DEV_DATA_PATH, "spotify_clean_track_files_parquet", "track_files.parquet")
    df_all_files = spark.read.parquet(files_src)
    write_single_parquet(
        df_all_files.join(dev_track_string_ids, df_all_files.track_id == dev_track_string_ids.dev_string_id, "inner").drop("dev_string_id"),
        files_dst
    )

    # --- 5. SMALL STANDALONE TABLES ---
    print("Copying available markets...")
    write_single_parquet(
        spark.read.parquet(os.path.join(base_src, "available_markets.parquet")),
        os.path.join(base_dst, "available_markets.parquet")
    )

    # --- 6. PLAYLISTS (Sample 50000) ---
    print("Sampling Playlists...")
    pl_src = os.path.join(SOURCE_DATA_PATH, "spotify_clean_playlists_parquet")
    pl_dst = os.path.join(DEV_DATA_PATH, "spotify_clean_playlists_parquet")
    
    df_dev_playlists = spark.read.parquet(os.path.join(pl_src, "playlists.parquet")).orderBy(col("followers_total").desc()).limit(50000)
    write_single_parquet(df_dev_playlists, os.path.join(pl_dst, "playlists.parquet"))
    
    dev_playlist_rowids = df_dev_playlists.select(col("rowid").alias("dev_playlist_rowid")).distinct()
    
    write_single_parquet(
        spark.read.parquet(os.path.join(pl_src, "playlist_tracks.parquet")).join(dev_playlist_rowids, col("playlist_rowid") == dev_playlist_rowids.dev_playlist_rowid, "inner").drop("dev_playlist_rowid"),
        os.path.join(pl_dst, "playlist_tracks.parquet")
    )

    write_single_parquet(
        spark.read.parquet(os.path.join(pl_src, "playlist_images.parquet")).join(dev_playlist_rowids, col("playlist_rowid") == dev_playlist_rowids.dev_playlist_rowid, "inner").drop("dev_playlist_rowid"),
        os.path.join(pl_dst, "playlist_images.parquet")
    )

    print("Dev Dataset creation complete!")
    spark.stop()

if __name__ == "__main__":
    main()