import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, md5, substring, when

# --- Configuration ---
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "/Volumes/metadata_datasets/spotify-dev-dataset")
LOCAL_LIMIT = 5000  # Set to None to process 100% of the data locally

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "host.docker.internal")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "spotify_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "root")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, table_name):
    start_time = time.time()
    print(f"Writing {table_name} to PostgreSQL...")
    df.write.jdbc(url=POSTGRES_URL, table=table_name, mode="overwrite", properties=POSTGRES_PROPERTIES)
    elapsed_time = time.time() - start_time
    print(f"Finished writing {table_name} in {elapsed_time:.3f} seconds.")

def generate_uuid_from_rowid(column_name):
    hash_col = md5(col(column_name).cast("string"))
    uuid_str = concat_ws("-", 
        substring(hash_col, 1, 8), substring(hash_col, 9, 4),
        substring(hash_col, 13, 4), substring(hash_col, 17, 4), substring(hash_col, 21, 12)
    )
    return when(col(column_name).isNotNull(), uuid_str).otherwise(lit(None))

def main():
    print("Initializing Local Spark Session (PostgreSQL with Referential Integrity)...")
    
    spark = SparkSession.builder \
        .appName("SpotifyLocalProcessing") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_parquet")
    playlists_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_playlists_parquet")
    audio_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_audio_features_parquet")
    files_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_track_files_parquet")
    redirects_json = os.path.join(LOCAL_DATA_PATH, "spotify_artist_redirects.json")

    print(f"Loading data from {LOCAL_DATA_PATH}...")
    
    df_artists = spark.read.parquet(os.path.join(base_parquet, "artists.parquet"))
    df_albums = spark.read.parquet(os.path.join(base_parquet, "albums.parquet"))
    df_tracks = spark.read.parquet(os.path.join(base_parquet, "tracks.parquet"))
    df_playlists = spark.read.parquet(os.path.join(playlists_parquet, "playlists.parquet"))
    df_markets = spark.read.parquet(os.path.join(base_parquet, "available_markets.parquet"))
    df_redirects = spark.read.json(redirects_json)

    df_audio = spark.read.parquet(os.path.join(audio_parquet, "track_audio_features.parquet"))
    df_track_files = spark.read.parquet(os.path.join(files_parquet, "track_files.parquet"))
    df_track_artists = spark.read.parquet(os.path.join(base_parquet, "track_artists.parquet"))
    df_artist_albums = spark.read.parquet(os.path.join(base_parquet, "artist_albums.parquet"))
    df_artist_genres = spark.read.parquet(os.path.join(base_parquet, "artist_genres.parquet"))
    df_playlist_tracks = spark.read.parquet(os.path.join(playlists_parquet, "playlist_tracks.parquet"))
    
    df_album_images = spark.read.parquet(os.path.join(base_parquet, "album_images.parquet"))
    df_artist_images = spark.read.parquet(os.path.join(base_parquet, "artist_images.parquet"))
    df_playlist_images = spark.read.parquet(os.path.join(playlists_parquet, "playlist_images.parquet"))

    # --- LOCAL SAMPLING WITH REFERENTIAL INTEGRITY ---
    if LOCAL_LIMIT:
        print(f"Applying local test limit of {LOCAL_LIMIT} rows per core entity while preserving relations...")
        df_artists = df_artists.limit(LOCAL_LIMIT)
        df_albums = df_albums.limit(LOCAL_LIMIT)
        df_tracks = df_tracks.limit(LOCAL_LIMIT)
        df_playlists = df_playlists.limit(LOCAL_LIMIT)
        df_markets = df_markets.limit(LOCAL_LIMIT)

        # Create aliased versions for matching foreign keys cleanly
        allowed_artists = df_artists.select(col("rowid").alias("artist_id_filter"))
        allowed_albums = df_albums.select(col("rowid").alias("album_id_filter"))
        allowed_tracks = df_tracks.select(col("rowid").alias("track_id_filter"))
        allowed_track_ids = df_tracks.select(col("id").alias("track_str_id_filter"))
        allowed_playlists = df_playlists.select(col("rowid").alias("playlist_id_filter"))

        # Explicit join conditions and drops to avoid duplicate column names
        df_track_artists = df_track_artists.join(allowed_tracks, df_track_artists.track_rowid == allowed_tracks.track_id_filter, "inner").drop("track_id_filter").join(allowed_artists, df_track_artists.artist_rowid == allowed_artists.artist_id_filter, "inner").drop("artist_id_filter")
            
        df_artist_albums = df_artist_albums.join(allowed_artists, df_artist_albums.artist_rowid == allowed_artists.artist_id_filter, "inner").drop("artist_id_filter").join(allowed_albums, df_artist_albums.album_rowid == allowed_albums.album_id_filter, "inner").drop("album_id_filter")
            
        df_artist_genres = df_artist_genres.join(allowed_artists, df_artist_genres.artist_rowid == allowed_artists.artist_id_filter, "inner").drop("artist_id_filter")
            
        df_playlist_tracks = df_playlist_tracks.join(allowed_playlists, df_playlist_tracks.playlist_rowid == allowed_playlists.playlist_id_filter, "inner").drop("playlist_id_filter")
            
        df_album_images = df_album_images.join(allowed_albums, df_album_images.album_rowid == allowed_albums.album_id_filter, "inner").drop("album_id_filter")
            
        df_artist_images = df_artist_images.join(allowed_artists, df_artist_images.artist_rowid == allowed_artists.artist_id_filter, "inner").drop("artist_id_filter")
            
        df_playlist_images = df_playlist_images.join(allowed_playlists, df_playlist_images.playlist_rowid == allowed_playlists.playlist_id_filter, "inner").drop("playlist_id_filter")
            
        df_audio = df_audio.join(allowed_track_ids, df_audio.track_id == allowed_track_ids.track_str_id_filter, "inner").drop("track_str_id_filter")
            
        df_track_files = df_track_files.join(allowed_track_ids, df_track_files.track_id == allowed_track_ids.track_str_id_filter, "inner").drop("track_str_id_filter")

    print("Applying transformations...")

    dim_artists = df_artists.select(generate_uuid_from_rowid("rowid").alias("id"), col("id").alias("spotify_id"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("name").alias("spotify_name"), col("followers_total").cast("long").alias("spotify_followers_total"), col("popularity").cast("integer").alias("spotify_popularity")).dropDuplicates(["id"])
    dim_albums = df_albums.select(generate_uuid_from_rowid("rowid").alias("id"), col("id").alias("spotify_id"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("name").alias("spotify_name"), col("album_type").alias("spotify_album_type"), generate_uuid_from_rowid("available_markets_rowid").alias("spotify_available_markets_id"), col("external_id_upc").alias("spotify_upc"), col("copyright_c").alias("spotify_copyright_c"), col("copyright_p").alias("spotify_copyright_p"), col("label").alias("spotify_label"), col("popularity").cast("integer").alias("spotify_popularity"), col("release_date").alias("spotify_release_date"), col("release_date_precision").alias("spotify_release_date_precision"), col("total_tracks").cast("integer").alias("spotify_total_tracks"), col("external_id_amgid").alias("spotify_amgid")).dropDuplicates(["id"])
    dim_tracks = df_tracks.select(generate_uuid_from_rowid("rowid").alias("id"), col("id").alias("spotify_id"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("name").alias("spotify_name"), col("preview_url").alias("spotify_preview_url"), generate_uuid_from_rowid("album_rowid").alias("album_id"), col("track_number").cast("integer").alias("spotify_track_number"), col("external_id_isrc").alias("spotify_isrc"), col("popularity").cast("integer").alias("spotify_popularity"), generate_uuid_from_rowid("available_markets_rowid").alias("spotify_available_markets_id"), col("disc_number").cast("integer").alias("spotify_disc_number"), col("duration_ms").cast("long").alias("spotify_duration_ms"), col("explicit").cast("boolean").alias("spotify_explicit")).dropDuplicates(["id"])
    dim_playlists = df_playlists.select(generate_uuid_from_rowid("rowid").alias("id"), col("id").alias("spotify_id"), col("snapshot_id").alias("spotify_snapshot_id"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("name").alias("spotify_name"), col("description").alias("spotify_description"), col("collaborative").cast("boolean").alias("spotify_collaborative"), col("public").cast("boolean").alias("spotify_public"), col("primary_color").alias("spotify_primary_color"), col("owner_id").alias("spotify_owner_id"), col("owner_display_name").alias("spotify_owner_display_name"), col("followers_total").cast("long").alias("spotify_followers_total"), col("tracks_total").cast("long").alias("spotify_tracks_total")).dropDuplicates(["id"])
    dim_markets = df_markets.select(generate_uuid_from_rowid("rowid").alias("id"), col("available_markets").alias("spotify_available_markets")).dropDuplicates(["id"])
    dim_artist_redirects = df_redirects.filter(col("from_id").isNotNull() & col("to_id").isNotNull()).select(col("from_id").alias("spotify_from_id"), col("to_id").alias("spotify_to_id"))
    
    df_audio_joined = df_audio.join(df_tracks.select(col("id"), col("rowid").alias("track_rowid")), df_audio.track_id == df_tracks.id, "inner")
    fact_track_audio_features = df_audio_joined.select(generate_uuid_from_rowid("rowid").alias("id"), generate_uuid_from_rowid("track_rowid").alias("track_id"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("null_response").cast("boolean").alias("spotify_null_response"), col("duration_ms").cast("long").alias("spotify_duration_ms"), col("time_signature").cast("integer").alias("spotify_time_signature"), col("tempo").cast("double").alias("spotify_tempo"), col("key").cast("integer").alias("spotify_key"), col("mode").cast("integer").alias("spotify_mode"), col("danceability").cast("double").alias("spotify_danceability"), col("energy").cast("double").alias("spotify_energy"), col("loudness").cast("double").alias("spotify_loudness"), col("speechiness").cast("double").alias("spotify_speechiness"), col("acousticness").cast("double").alias("spotify_acousticness"), col("instrumentalness").cast("double").alias("spotify_instrumentalness"), col("liveness").cast("double").alias("spotify_liveness"), col("valence").cast("double").alias("spotify_valence")).dropDuplicates(["id"])
    
    df_files_joined = df_track_files.join(df_tracks.select(col("id"), col("rowid").alias("track_rowid")), df_track_files.track_id == df_tracks.id, "inner")
    fact_track_files = df_files_joined.select(generate_uuid_from_rowid("rowid").alias("id"), generate_uuid_from_rowid("track_rowid").alias("track_id"), col("track_id").alias("spotify_track_id"), col("filename").alias("spotify_filename"), col("reencoded_kbit_vbr").cast("integer").alias("spotify_reencoded_kbit_vbr"), col("fetched_at").cast("long").alias("spotify_fetched_at"), col("session_country").alias("spotify_session_country"), col("sha256_original").alias("spotify_sha256_original"), col("sha256_with_embedded_meta").alias("spotify_sha256_with_embedded_meta"), col("status").alias("spotify_status"), col("isrc_has_download").cast("boolean").alias("spotify_isrc_has_download"), col("track_popularity").cast("integer").alias("spotify_track_popularity"), col("secondary_priority").cast("double").alias("spotify_secondary_priority"), col("prefixed_ogg_packet").cast("binary").alias("spotify_prefixed_ogg_packet"), col("alternatives").alias("spotify_alternatives"), col("file_id_ogg_vorbis_96").alias("spotify_file_id_ogg_vorbis_96"), col("file_id_ogg_vorbis_160").alias("spotify_file_id_ogg_vorbis_160"), col("file_id_ogg_vorbis_320").alias("spotify_file_id_ogg_vorbis_320"), col("file_id_aac_24").alias("spotify_file_id_aac_24"), col("language_of_performance").alias("spotify_language_of_performance"), col("artist_roles").alias("spotify_artist_roles"), col("has_lyrics").cast("boolean").alias("spotify_has_lyrics"), col("licensor").alias("spotify_licensor"), col("original_title").alias("spotify_original_title"), col("version_title").alias("spotify_version_title"), col("file_id_mp3_96").alias("spotify_file_id_mp3_96"), col("content_ratings").alias("spotify_content_ratings"), col("filesize_bytes").cast("long").alias("spotify_filesize_bytes")).dropDuplicates(["id"])

    bridge_track_artists = df_track_artists.select(generate_uuid_from_rowid("track_rowid").alias("track_id"), generate_uuid_from_rowid("artist_rowid").alias("artist_id")).dropDuplicates()
    bridge_artist_albums = df_artist_albums.select(generate_uuid_from_rowid("artist_rowid").alias("artist_id"), generate_uuid_from_rowid("album_rowid").alias("album_id"), col("is_appears_on").cast("boolean").alias("spotify_is_appears_on"), col("is_implicit_appears_on").cast("boolean").alias("spotify_is_implicit_appears_on"), col("index_in_album").cast("integer").alias("spotify_index_in_album")).dropDuplicates()
    bridge_artist_genres = df_artist_genres.select(generate_uuid_from_rowid("artist_rowid").alias("artist_id"), col("genre").alias("spotify_genre")).dropDuplicates()
    fact_playlist_tracks = df_playlist_tracks.select(generate_uuid_from_rowid("playlist_rowid").alias("playlist_id"), col("position").cast("integer").alias("spotify_position"), col("is_episode").cast("boolean").alias("spotify_is_episode"), generate_uuid_from_rowid("track_rowid").alias("track_id"), col("id_if_not_in_tracks_table").alias("spotify_id_if_not_in_tracks_table"), col("added_at").cast("long").alias("spotify_added_at"), col("added_by_id").alias("spotify_added_by_id"), col("primary_color").alias("spotify_primary_color"), col("video_thumbnail_url").alias("spotify_video_thumbnail_url"), col("is_local").cast("boolean").alias("spotify_is_local"), col("name_if_is_local").alias("spotify_name_if_is_local"), col("uri_if_is_local").alias("spotify_uri_if_is_local"), col("album_name_if_is_local").alias("spotify_album_name_if_is_local"), col("artists_name_if_is_local").alias("spotify_artists_name_if_is_local"), col("duration_ms_if_is_local").cast("long").alias("spotify_duration_ms_if_is_local"))
    sat_artist_images = df_artist_images.select(generate_uuid_from_rowid("artist_rowid").alias("artist_id"), col("width").cast("integer").alias("spotify_width"), col("height").cast("integer").alias("spotify_height"), col("url").alias("spotify_url"))
    sat_album_images = df_album_images.select(generate_uuid_from_rowid("album_rowid").alias("album_id"), col("width").cast("integer").alias("spotify_width"), col("height").cast("integer").alias("spotify_height"), col("url").alias("spotify_url"))
    sat_playlist_images = df_playlist_images.select(generate_uuid_from_rowid("playlist_rowid").alias("playlist_id"), col("width").cast("integer").alias("spotify_width"), col("height").cast("integer").alias("spotify_height"), col("url").alias("spotify_url"))

    # Load to Postgres
    write_to_postgres(dim_artist_redirects, "dim_artist_redirects")
    write_to_postgres(dim_markets, "dim_markets")
    write_to_postgres(dim_artists, "dim_artists")
    write_to_postgres(dim_albums, "dim_albums")
    write_to_postgres(dim_tracks, "dim_tracks")
    write_to_postgres(dim_playlists, "dim_playlists")
    write_to_postgres(fact_track_audio_features, "fact_track_audio_features")
    write_to_postgres(fact_track_files, "fact_track_files")
    write_to_postgres(bridge_track_artists, "bridge_track_artists")
    write_to_postgres(bridge_artist_albums, "bridge_artist_albums")
    write_to_postgres(bridge_artist_genres, "bridge_artist_genres")
    write_to_postgres(sat_artist_images, "sat_artist_images")
    write_to_postgres(sat_album_images, "sat_album_images")
    write_to_postgres(sat_playlist_images, "sat_playlist_images")
    write_to_postgres(fact_playlist_tracks, "fact_playlist_tracks")

    print("Local pipeline execution complete.")
    spark.stop()

if __name__ == "__main__":
    main()