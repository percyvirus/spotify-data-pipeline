import argparse
import os
import time
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, md5, substring, when

# --- Configuration ---
LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH", "/mnt/hdd_jetson/datasets/spotify-metadata-by-annas-archive")
SPARK_TEMP_DIR = os.getenv("SPARK_TEMP_DIR", "/mnt/hdd_jetson/spark-temp")
HDD_TABLESPACE_DIR = os.getenv("HDD_TABLESPACE_DIR", "/mnt/hdd_jetson/pg_hdd_tablespace")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "spotify_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "root")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}?rewriteBatchedInserts=true"
POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "10000",
    "isolationLevel": "NONE",
    "truncate": "true"
}


def write_to_postgres(df, table_name):
    start_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Writing {table_name} to PostgreSQL...", flush=True)
    df.coalesce(1).write.jdbc(
        url=POSTGRES_JDBC_URL,
        table=table_name,
        mode="overwrite",
        properties=POSTGRES_PROPERTIES
    )
    elapsed_time = time.time() - start_time
    print(f"[{time.strftime('%H:%M:%S')}] Finished {table_name} in {elapsed_time / 60:.2f} min.", flush=True)


def generate_uuid_from_rowid(column_name):
    hash_col = md5(col(column_name).cast("string"))
    uuid_str = concat_ws(
        "-",
        substring(hash_col, 1, 8),
        substring(hash_col, 9, 4),
        substring(hash_col, 13, 4),
        substring(hash_col, 17, 4),
        substring(hash_col, 21, 12)
    )
    return when(col(column_name).isNotNull(), uuid_str).otherwise(lit(None))


def optimize_postgres_storage_and_indexes(include_playlists: bool):
    """Configure pgvector, move secondary tables to HDD tablespace, and build indexes."""
    print(f"[{time.strftime('%H:%M:%S')}] Applying PostgreSQL tablespace routing and indexes...", flush=True)

    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            # 1. Enable pgvector extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

            # 2. Check or create hdd_space tablespace if directory exists
            cur.execute("SELECT 1 FROM pg_tablespace WHERE spcname = 'hdd_space';")
            has_hdd_space = cur.fetchone() is not None

            if not has_hdd_space and os.path.isdir(HDD_TABLESPACE_DIR):
                try:
                    cur.execute(f"CREATE TABLESPACE hdd_space LOCATION '{HDD_TABLESPACE_DIR}';")
                    has_hdd_space = True
                    print("[INFO] Created PostgreSQL tablespace 'hdd_space'.", flush=True)
                except Exception as exc:
                    print(f"[WARN] Could not create 'hdd_space' tablespace ({exc}). Using default NVMe storage.", flush=True)

            # 3. Move heavy secondary tables to HDD to preserve NVMe capacity
            if has_hdd_space:
                hdd_tables = [
                    "fact_track_files",
                    "sat_album_images",
                    "dim_albums",
                    "bridge_artist_albums",
                    "sat_artist_images",
                    "bridge_artist_genres",
                    "dim_artist_redirects"
                ]
                for table in hdd_tables:
                    print(f"[{time.strftime('%H:%M:%S')}] Moving {table} to hdd_space...", flush=True)
                    cur.execute(f"ALTER TABLE {table} SET TABLESPACE hdd_space;")

            # 4. Primary keys and low-latency indexes on NVMe (pg_default)
            nvme_ddl = [
                "ALTER TABLE dim_artists ADD PRIMARY KEY (id);",
                "ALTER TABLE dim_tracks ADD PRIMARY KEY (id);",
                "ALTER TABLE dim_markets ADD PRIMARY KEY (id);",
                "ALTER TABLE fact_track_audio_features ADD PRIMARY KEY (id);",
                "CREATE INDEX IF NOT EXISTS idx_dim_tracks_spotify_id ON dim_tracks(spotify_id);",
                "CREATE INDEX IF NOT EXISTS idx_dim_tracks_isrc ON dim_tracks(spotify_isrc);",
                "CREATE INDEX IF NOT EXISTS idx_fact_audio_track ON fact_track_audio_features(track_id);",
                "CREATE INDEX IF NOT EXISTS idx_bridge_track_artists_track ON bridge_track_artists(track_id);",
                "CREATE INDEX IF NOT EXISTS idx_bridge_track_artists_artist ON bridge_track_artists(artist_id);"
            ]
            for stmt in nvme_ddl:
                print(f"[{time.strftime('%H:%M:%S')}] Executing: {stmt}", flush=True)
                cur.execute(stmt)

            # 5. Primary keys and heavy indexes routed to HDD (if available)
            ts_clause_pk = "USING INDEX TABLESPACE hdd_space" if has_hdd_space else ""
            ts_clause_idx = "TABLESPACE hdd_space" if has_hdd_space else ""

            hdd_ddl = [
                f"ALTER TABLE fact_track_files ADD PRIMARY KEY (id) {ts_clause_pk};",
                f"ALTER TABLE dim_albums ADD PRIMARY KEY (id) {ts_clause_pk};",
                f"CREATE INDEX IF NOT EXISTS idx_fact_files_track ON fact_track_files(track_id) {ts_clause_idx};",
                f"CREATE INDEX IF NOT EXISTS idx_dim_tracks_album ON dim_tracks(album_id) {ts_clause_idx};",
                f"CREATE INDEX IF NOT EXISTS idx_dim_albums_market ON dim_albums(spotify_available_markets_id) {ts_clause_idx};",
                f"CREATE INDEX IF NOT EXISTS idx_bridge_artist_albums_artist ON bridge_artist_albums(artist_id) {ts_clause_idx};",
                f"CREATE INDEX IF NOT EXISTS idx_bridge_artist_albums_album ON bridge_artist_albums(album_id) {ts_clause_idx};",
                f"CREATE INDEX IF NOT EXISTS idx_bridge_artist_genres_artist ON bridge_artist_genres(artist_id) {ts_clause_idx};"
            ]
            for stmt in hdd_ddl:
                print(f"[{time.strftime('%H:%M:%S')}] Executing: {stmt}", flush=True)
                cur.execute(stmt)

            # 6. Optional playlist indexes
            if include_playlists:
                playlist_ddl = [
                    f"ALTER TABLE dim_playlists ADD PRIMARY KEY (id) {ts_clause_pk};",
                    f"CREATE INDEX IF NOT EXISTS idx_fact_playlist_tracks_playlist ON fact_playlist_tracks(playlist_id) {ts_clause_idx};",
                    f"CREATE INDEX IF NOT EXISTS idx_fact_playlist_tracks_track ON fact_playlist_tracks(track_id) {ts_clause_idx};"
                ]
                for stmt in playlist_ddl:
                    print(f"[{time.strftime('%H:%M:%S')}] Executing: {stmt}", flush=True)
                    cur.execute(stmt)

            print(f"[{time.strftime('%H:%M:%S')}] Running ANALYZE...", flush=True)
            cur.execute("ANALYZE;")

    print(f"[{time.strftime('%H:%M:%S')}] PostgreSQL storage routing and indexing complete.", flush=True)


def parse_args():
    env_default = os.getenv("INCLUDE_PLAYLISTS", "false").strip().lower() in ("1", "true", "yes")
    parser = argparse.ArgumentParser(description="Memory-Tuned Spotify Catalog ETL for NVIDIA Jetson Orin Nano")
    parser.add_argument(
        "--include-playlists",
        action="store_true",
        default=env_default,
        help="Include playlist tables (dim_playlists, sat_playlist_images, fact_playlist_tracks - ~1.7B rows)"
    )
    parser.add_argument(
        "--skip-post-sql",
        action="store_true",
        default=False,
        help="Skip tablespace routing and index creation after Spark ingestion"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(SPARK_TEMP_DIR, exist_ok=True)
    print(
        f"Initializing Memory-Tuned Spark Session for Jetson Orin Nano (include_playlists={args.include_playlists})...",
        flush=True
    )

    spark = SparkSession.builder \
        .appName("SpotifyJetsonFullIngest") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.driver.memory", "2048m") \
        .config("spark.driver.memoryOverhead", "512m") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.memory.fraction", "0.5") \
        .config("spark.sql.shuffle.partitions", "512") \
        .config("spark.sql.files.maxPartitionBytes", "33554432") \
        .config("spark.local.dir", SPARK_TEMP_DIR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_parquet")
    playlists_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_playlists_parquet")
    audio_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_audio_features_parquet")
    files_parquet = os.path.join(LOCAL_DATA_PATH, "spotify_clean_track_files_parquet")
    redirects_json = os.path.join(LOCAL_DATA_PATH, "spotify_artist_redirects.json")

    # 1. Core Dimensions
    df_redirects = spark.read.json(redirects_json)
    dim_artist_redirects = df_redirects.filter(col("from_id").isNotNull() & col("to_id").isNotNull()) \
        .select(col("from_id").alias("spotify_from_id"), col("to_id").alias("spotify_to_id"))
    write_to_postgres(dim_artist_redirects, "dim_artist_redirects")

    df_markets = spark.read.parquet(os.path.join(base_parquet, "available_markets.parquet"))
    dim_markets = df_markets.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        col("available_markets").alias("spotify_available_markets")
    )
    write_to_postgres(dim_markets, "dim_markets")

    df_artists = spark.read.parquet(os.path.join(base_parquet, "artists.parquet"))
    dim_artists = df_artists.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        col("id").alias("spotify_id"),
        col("fetched_at").cast("long").alias("spotify_fetched_at"),
        col("name").alias("spotify_name"),
        col("followers_total").cast("long").alias("spotify_followers_total"),
        col("popularity").cast("integer").alias("spotify_popularity")
    )
    write_to_postgres(dim_artists, "dim_artists")

    df_albums = spark.read.parquet(os.path.join(base_parquet, "albums.parquet"))
    dim_albums = df_albums.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        col("id").alias("spotify_id"),
        col("fetched_at").cast("long").alias("spotify_fetched_at"),
        col("name").alias("spotify_name"),
        col("album_type").alias("spotify_album_type"),
        generate_uuid_from_rowid("available_markets_rowid").alias("spotify_available_markets_id"),
        col("external_id_upc").alias("spotify_upc"),
        col("copyright_c").alias("spotify_copyright_c"),
        col("copyright_p").alias("spotify_copyright_p"),
        col("label").alias("spotify_label"),
        col("popularity").cast("integer").alias("spotify_popularity"),
        col("release_date").alias("spotify_release_date"),
        col("release_date_precision").alias("spotify_release_date_precision"),
        col("total_tracks").cast("integer").alias("spotify_total_tracks"),
        col("external_id_amgid").alias("spotify_amgid")
    )
    write_to_postgres(dim_albums, "dim_albums")

    df_tracks = spark.read.parquet(os.path.join(base_parquet, "tracks.parquet"))
    dim_tracks = df_tracks.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        col("id").alias("spotify_id"),
        col("fetched_at").cast("long").alias("spotify_fetched_at"),
        col("name").alias("spotify_name"),
        col("preview_url").alias("spotify_preview_url"),
        generate_uuid_from_rowid("album_rowid").alias("album_id"),
        col("track_number").cast("integer").alias("spotify_track_number"),
        col("external_id_isrc").alias("spotify_isrc"),
        col("popularity").cast("integer").alias("spotify_popularity"),
        generate_uuid_from_rowid("available_markets_rowid").alias("spotify_available_markets_id"),
        col("disc_number").cast("integer").alias("spotify_disc_number"),
        col("duration_ms").cast("long").alias("spotify_duration_ms"),
        col("explicit").cast("boolean").alias("spotify_explicit")
    )
    write_to_postgres(dim_tracks, "dim_tracks")

    # 2. Facts requiring join with tracks
    tracks_lookup = df_tracks.select(col("id"), col("rowid").alias("track_rowid"))

    df_audio = spark.read.parquet(os.path.join(audio_parquet, "track_audio_features.parquet"))
    df_audio_joined = df_audio.join(tracks_lookup, df_audio.track_id == tracks_lookup.id, "inner")
    fact_track_audio_features = df_audio_joined.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        generate_uuid_from_rowid("track_rowid").alias("track_id"),
        col("fetched_at").cast("long").alias("spotify_fetched_at"),
        col("null_response").cast("boolean").alias("spotify_null_response"),
        col("duration_ms").cast("long").alias("spotify_duration_ms"),
        col("time_signature").cast("integer").alias("spotify_time_signature"),
        col("tempo").cast("double").alias("spotify_tempo"),
        col("key").cast("integer").alias("spotify_key"),
        col("mode").cast("integer").alias("spotify_mode"),
        col("danceability").cast("double").alias("spotify_danceability"),
        col("energy").cast("double").alias("spotify_energy"),
        col("loudness").cast("double").alias("spotify_loudness"),
        col("speechiness").cast("double").alias("spotify_speechiness"),
        col("acousticness").cast("double").alias("spotify_acousticness"),
        col("instrumentalness").cast("double").alias("spotify_instrumentalness"),
        col("liveness").cast("double").alias("spotify_liveness"),
        col("valence").cast("double").alias("spotify_valence")
    )
    write_to_postgres(fact_track_audio_features, "fact_track_audio_features")

    df_track_files = spark.read.parquet(os.path.join(files_parquet, "track_files.parquet"))
    df_files_joined = df_track_files.join(tracks_lookup, df_track_files.track_id == tracks_lookup.id, "inner")
    fact_track_files = df_files_joined.select(
        generate_uuid_from_rowid("rowid").alias("id"),
        generate_uuid_from_rowid("track_rowid").alias("track_id"),
        col("track_id").alias("spotify_track_id"),
        col("filename").alias("spotify_filename"),
        col("reencoded_kbit_vbr").cast("integer").alias("spotify_reencoded_kbit_vbr"),
        col("fetched_at").cast("long").alias("spotify_fetched_at"),
        col("session_country").alias("spotify_session_country"),
        col("sha256_original").alias("spotify_sha256_original"),
        col("sha256_with_embedded_meta").alias("spotify_sha256_with_embedded_meta"),
        col("status").alias("spotify_status"),
        col("isrc_has_download").cast("boolean").alias("spotify_isrc_has_download"),
        col("track_popularity").cast("integer").alias("spotify_track_popularity"),
        col("secondary_priority").cast("double").alias("spotify_secondary_priority"),
        col("prefixed_ogg_packet").cast("binary").alias("spotify_prefixed_ogg_packet"),
        col("alternatives").alias("spotify_alternatives"),
        col("file_id_ogg_vorbis_96").alias("spotify_file_id_ogg_vorbis_96"),
        col("file_id_ogg_vorbis_160").alias("spotify_file_id_ogg_vorbis_160"),
        col("file_id_ogg_vorbis_320").alias("spotify_file_id_ogg_vorbis_320"),
        col("file_id_aac_24").alias("spotify_file_id_aac_24"),
        col("language_of_performance").alias("spotify_language_of_performance"),
        col("artist_roles").alias("spotify_artist_roles"),
        col("has_lyrics").cast("boolean").alias("spotify_has_lyrics"),
        col("licensor").alias("spotify_licensor"),
        col("original_title").alias("spotify_original_title"),
        col("version_title").alias("spotify_version_title"),
        col("file_id_mp3_96").alias("spotify_file_id_mp3_96"),
        col("content_ratings").alias("spotify_content_ratings"),
        col("filesize_bytes").cast("long").alias("spotify_filesize_bytes")
    )
    write_to_postgres(fact_track_files, "fact_track_files")

    # 3. Bridges & Satellites
    df_track_artists = spark.read.parquet(os.path.join(base_parquet, "track_artists.parquet"))
    write_to_postgres(
        df_track_artists.select(
            generate_uuid_from_rowid("track_rowid").alias("track_id"),
            generate_uuid_from_rowid("artist_rowid").alias("artist_id")
        ),
        "bridge_track_artists"
    )

    df_artist_albums = spark.read.parquet(os.path.join(base_parquet, "artist_albums.parquet"))
    write_to_postgres(
        df_artist_albums.select(
            generate_uuid_from_rowid("artist_rowid").alias("artist_id"),
            generate_uuid_from_rowid("album_rowid").alias("album_id"),
            col("is_appears_on").cast("boolean").alias("spotify_is_appears_on"),
            col("is_implicit_appears_on").cast("boolean").alias("spotify_is_implicit_appears_on"),
            col("index_in_album").cast("integer").alias("spotify_index_in_album")
        ),
        "bridge_artist_albums"
    )

    df_artist_genres = spark.read.parquet(os.path.join(base_parquet, "artist_genres.parquet"))
    write_to_postgres(
        df_artist_genres.select(
            generate_uuid_from_rowid("artist_rowid").alias("artist_id"),
            col("genre").alias("spotify