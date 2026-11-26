import argparse
import os
import tarfile
import time
import urllib.request
import psycopg

# --- Configuration ---
MB_BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport"
DOWNLOAD_DIR = os.getenv("MB_DOWNLOAD_DIR", "./data/musicbrainz_dumps")
MB_POSTGRES_HOST = os.getenv("MB_POSTGRES_HOST", "localhost")
MB_POSTGRES_PORT = os.getenv("MB_POSTGRES_PORT", "5433")
MB_POSTGRES_DB = os.getenv("MB_POSTGRES_DB", "musicbrainz_db")
MB_POSTGRES_USER = os.getenv("MB_POSTGRES_USER", "musicbrainz")
MB_POSTGRES_PASSWORD = os.getenv("MB_POSTGRES_PASSWORD", "musicbrainz")

POSTGRES_DSN = (
    f"postgresql://{MB_POSTGRES_USER}:{MB_POSTGRES_PASSWORD}"
    f"@{MB_POSTGRES_HOST}:{MB_POSTGRES_PORT}/{MB_POSTGRES_DB}"
)

# Core MusicBrainz tables required for tracks, ISRCs, works, ISWCs, and artist credits/roles
MB_DDL = """
DROP SCHEMA IF EXISTS musicbrainz CASCADE;
CREATE SCHEMA musicbrainz;
SET search_path TO musicbrainz;

CREATE UNLOGGED TABLE language (
    id INTEGER NOT NULL,
    iso_code_2t CHAR(3),
    iso_code_2b CHAR(3),
    iso_code_1 CHAR(2),
    name VARCHAR(100) NOT NULL,
    frequency INTEGER NOT NULL DEFAULT 0,
    iso_code_3 CHAR(3)
);

CREATE UNLOGGED TABLE work_type (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    parent INTEGER,
    child_order INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    gid UUID NOT NULL
);

CREATE UNLOGGED TABLE artist_type (
    id INTEGER NOT NULL,
    name VARCHAR(255) NOT NULL,
    parent INTEGER,
    child_order INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    gid UUID NOT NULL
);

CREATE UNLOGGED TABLE area (
    id INTEGER NOT NULL,
    gid UUID NOT NULL,
    name VARCHAR NOT NULL,
    type INTEGER,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    begin_date_year SMALLINT,
    begin_date_month SMALLINT,
    begin_date_day SMALLINT,
    end_date_year SMALLINT,
    end_date_month SMALLINT,
    end_date_day SMALLINT,
    ended BOOLEAN NOT NULL DEFAULT FALSE,
    comment VARCHAR(255) NOT NULL DEFAULT ''
);

CREATE UNLOGGED TABLE artist (
    id INTEGER NOT NULL,
    gid UUID NOT NULL,
    name VARCHAR NOT NULL,
    sort_name VARCHAR NOT NULL,
    begin_date_year SMALLINT,
    begin_date_month SMALLINT,
    begin_date_day SMALLINT,
    end_date_year SMALLINT,
    end_date_month SMALLINT,
    end_date_day SMALLINT,
    type INTEGER,
    area INTEGER,
    gender INTEGER,
    comment VARCHAR(255) NOT NULL DEFAULT '',
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    ended BOOLEAN NOT NULL DEFAULT FALSE,
    begin_area INTEGER,
    end_area INTEGER
);

CREATE UNLOGGED TABLE artist_credit (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    artist_count SMALLINT NOT NULL,
    ref_count INTEGER DEFAULT 0,
    created TIMESTAMPTZ,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    gid UUID NOT NULL
);

CREATE UNLOGGED TABLE artist_credit_name (
    artist_credit INTEGER NOT NULL,
    position SMALLINT NOT NULL,
    artist INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    join_phrase TEXT NOT NULL DEFAULT ''
);

CREATE UNLOGGED TABLE recording (
    id INTEGER NOT NULL,
    gid UUID NOT NULL,
    name VARCHAR NOT NULL,
    artist_credit INTEGER NOT NULL,
    length INTEGER,
    comment VARCHAR(255) NOT NULL DEFAULT '',
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    video BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNLOGGED TABLE isrc (
    id INTEGER NOT NULL,
    recording INTEGER NOT NULL,
    isrc CHAR(12) NOT NULL,
    source SMALLINT,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMPTZ
);

CREATE UNLOGGED TABLE work (
    id INTEGER NOT NULL,
    gid UUID NOT NULL,
    name VARCHAR NOT NULL,
    type INTEGER,
    comment VARCHAR(255) NOT NULL DEFAULT '',
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ
);

CREATE UNLOGGED TABLE work_language (
    work INTEGER NOT NULL,
    language INTEGER NOT NULL,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMPTZ
);

CREATE UNLOGGED TABLE iswc (
    id INTEGER NOT NULL,
    work INTEGER NOT NULL,
    iswc CHARACTER(15),
    source SMALLINT,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMPTZ
);

CREATE UNLOGGED TABLE link_type (
    id INTEGER NOT NULL,
    parent INTEGER,
    child_order INTEGER NOT NULL DEFAULT 0,
    gid UUID NOT NULL,
    entity_type0 VARCHAR(50) NOT NULL,
    entity_type1 VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    link_phrase VARCHAR(255) NOT NULL,
    reverse_link_phrase VARCHAR(255) NOT NULL,
    long_link_phrase VARCHAR(255) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    is_deprecated BOOLEAN NOT NULL DEFAULT FALSE,
    has_dates BOOLEAN NOT NULL DEFAULT TRUE,
    entity0_cardinality INTEGER NOT NULL DEFAULT 0,
    entity1_cardinality INTEGER NOT NULL DEFAULT 0
);

CREATE UNLOGGED TABLE link_attribute_type (
    id INTEGER NOT NULL,
    parent INTEGER,
    root INTEGER NOT NULL,
    child_order INTEGER NOT NULL DEFAULT 0,
    gid UUID NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    last_updated TIMESTAMPTZ
);

CREATE UNLOGGED TABLE link (
    id INTEGER NOT NULL,
    link_type INTEGER NOT NULL,
    begin_date_year SMALLINT,
    begin_date_month SMALLINT,
    begin_date_day SMALLINT,
    end_date_year SMALLINT,
    end_date_month SMALLINT,
    end_date_day SMALLINT,
    attribute_count INTEGER NOT NULL DEFAULT 0,
    created TIMESTAMPTZ,
    ended BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNLOGGED TABLE link_attribute (
    link INTEGER NOT NULL,
    attribute_type INTEGER NOT NULL,
    created TIMESTAMPTZ
);

CREATE UNLOGGED TABLE l_recording_work (
    id INTEGER NOT NULL,
    link INTEGER NOT NULL,
    entity0 INTEGER NOT NULL,
    entity1 INTEGER NOT NULL,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    link_order INTEGER NOT NULL DEFAULT 0,
    entity0_credit TEXT NOT NULL DEFAULT '',
    entity1_credit TEXT NOT NULL DEFAULT ''
);

CREATE UNLOGGED TABLE l_artist_recording (
    id INTEGER NOT NULL,
    link INTEGER NOT NULL,
    entity0 INTEGER NOT NULL,
    entity1 INTEGER NOT NULL,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    link_order INTEGER NOT NULL DEFAULT 0,
    entity0_credit TEXT NOT NULL DEFAULT '',
    entity1_credit TEXT NOT NULL DEFAULT ''
);

CREATE UNLOGGED TABLE l_artist_work (
    id INTEGER NOT NULL,
    link INTEGER NOT NULL,
    entity0 INTEGER NOT NULL,
    entity1 INTEGER NOT NULL,
    edits_pending INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ,
    link_order INTEGER NOT NULL DEFAULT 0,
    entity0_credit TEXT NOT NULL DEFAULT '',
    entity1_credit TEXT NOT NULL DEFAULT ''
);

CREATE UNLOGGED TABLE recording_meta (
    id INTEGER NOT NULL,
    rating SMALLINT,
    rating_count INTEGER
);

CREATE UNLOGGED TABLE work_meta (
    id INTEGER NOT NULL,
    rating SMALLINT,
    rating_count INTEGER
);
"""

MB_INDEXES_AND_VIEWS = """
SET search_path TO musicbrainz;

-- Primary keys & lookup indexes for fast cross-database joins and exploration
ALTER TABLE language ADD PRIMARY KEY (id);
ALTER TABLE work_type ADD PRIMARY KEY (id);
ALTER TABLE artist_type ADD PRIMARY KEY (id);
ALTER TABLE area ADD PRIMARY KEY (id);
ALTER TABLE artist ADD PRIMARY KEY (id);
ALTER TABLE artist_credit ADD PRIMARY KEY (id);
ALTER TABLE recording ADD PRIMARY KEY (id);
ALTER TABLE work ADD PRIMARY KEY (id);
ALTER TABLE link_type ADD PRIMARY KEY (id);
ALTER TABLE link_attribute_type ADD PRIMARY KEY (id);
ALTER TABLE link ADD PRIMARY KEY (id);

CREATE INDEX idx_mb_isrc_code ON isrc(isrc);
CREATE INDEX idx_mb_isrc_recording ON isrc(recording);
CREATE INDEX idx_mb_iswc_work ON iswc(work);
CREATE INDEX idx_mb_iswc_code ON iswc(iswc);
CREATE INDEX idx_mb_recording_gid ON recording(gid);
CREATE INDEX idx_mb_recording_ac ON recording(artist_credit);
CREATE INDEX idx_mb_acn_ac ON artist_credit_name(artist_credit);
CREATE INDEX idx_mb_acn_artist ON artist_credit_name(artist);
CREATE INDEX idx_mb_work_gid ON work(gid);
CREATE INDEX idx_mb_work_lang_work ON work_language(work);
CREATE INDEX idx_mb_lrw_rec ON l_recording_work(entity0);
CREATE INDEX idx_mb_lrw_work ON l_recording_work(entity1);
CREATE INDEX idx_mb_lar_rec ON l_artist_recording(entity1);
CREATE INDEX idx_mb_lar_artist ON l_artist_recording(entity0);
CREATE INDEX idx_mb_law_work ON l_artist_work(entity1);
CREATE INDEX idx_mb_law_artist ON l_artist_work(entity0);
CREATE INDEX idx_mb_link_attr_link ON link_attribute(link);

-- Helper view to inspect recording -> work relationships with cover/live/medley flags
CREATE OR REPLACE VIEW v_recording_works_enriched AS
SELECT
    r.id AS recording_id,
    r.gid AS recording_mbid,
    r.name AS recording_title,
    w.id AS work_id,
    w.gid AS work_mbid,
    w.name AS work_title,
    wt.name AS work_type,
    lt.name AS link_type,
    COALESCE(BOOL_OR(lat.name = 'cover'), FALSE) AS is_cover,
    COALESCE(BOOL_OR(lat.name = 'live'), FALSE) AS is_live,
    COALESCE(BOOL_OR(lat.name = 'medley'), FALSE) AS is_medley,
    COALESCE(BOOL_OR(lat.name = 'instrumental'), FALSE) AS is_instrumental
FROM l_recording_work lrw
JOIN recording r ON r.id = lrw.entity0
JOIN work w ON w.id = lrw.entity1
LEFT JOIN work_type wt ON wt.id = w.type
JOIN link l ON l.id = lrw.link
JOIN link_type lt ON lt.id = l.link_type
LEFT JOIN link_attribute la ON la.link = l.id
LEFT JOIN link_attribute_type lat ON lat.id = la.attribute_type
GROUP BY r.id, r.gid, r.name, w.id, w.gid, w.name, wt.name, lt.name;

ANALYZE;
"""

CORE_TABLES = {
    "language",
    "work_type",
    "artist_type",
    "area",
    "artist",
    "artist_credit",
    "artist_credit_name",
    "recording",
    "isrc",
    "work",
    "work_language",
    "iswc",
    "link_type",
    "link_attribute_type",
    "link",
    "link_attribute",
    "l_recording_work",
    "l_artist_recording",
    "l_artist_work",
}

DERIVED_TABLES = {
    "recording_meta",
    "work_meta",
}


def get_latest_dump_tag() -> str:
    latest_url = f"{MB_BASE_URL}/LATEST"
    with urllib.request.urlopen(latest_url, timeout=30) as resp:
        return resp.read().decode("utf-8").strip()


def download_file_if_needed(url: str, dest_path: str):
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=30) as resp:
        remote_size = int(resp.headers.get("Content-Length", 0))

    if os.path.exists(dest_path) and os.path.getsize(dest_path) == remote_size and remote_size > 0:
        print(f"[SKIP] {os.path.basename(dest_path)} already downloaded ({remote_size / (1024**3):.2f} GB).", flush=True)
        return

    print(f"[DOWNLOAD] Fetching {url} ({remote_size / (1024**3):.2f} GB)...", flush=True)
    start_time = time.time()
    downloaded = 0
    chunk_size = 8 * 1024 * 1024  # 8 MB chunks

    with urllib.request.urlopen(url, timeout=60) as resp, open(dest_path, "wb") as out_f:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            out_f.write(chunk)
            downloaded += len(chunk)
            if downloaded % (256 * 1024 * 1024) == 0:
                pct = (downloaded / remote_size * 100) if remote_size else 0
                print(
                    f"  -> {os.path.basename(dest_path)}: {downloaded / (1024**3):.2f} GB ({pct:.1f}%)",
                    flush=True,
                )

    elapsed = (time.time() - start_time) / 60
    print(f"[DONE] Downloaded {os.path.basename(dest_path)} in {elapsed:.2f} min.", flush=True)


def stream_tar_to_postgres(tar_path: str, target_tables: set[str], conn: psycopg.Connection):
    print(f"[{time.strftime('%H:%M:%S')}] Streaming tables from {os.path.basename(tar_path)}...", flush=True)
    with tarfile.open(tar_path, mode="r:bz2") as tf:
        for member in tf:
            if not member.isfile():
                continue
            # Members are stored under 'mbdump/<table_name>'
            table_name = os.path.basename(member.name)
            if table_name not in target_tables:
                continue

            start_t = time.time()
            print(f"[{time.strftime('%H:%M:%S')}] COPY musicbrainz.{table_name}...", flush=True)
            extracted = tf.extractfile(member)
            if extracted is None:
                continue

            with conn.cursor() as cur:
                copy_sql = f"COPY musicbrainz.{table_name} FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
                with cur.copy(copy_sql) as copy:
                    while True:
                        block = extracted.read(4 * 1024 * 1024)
                        if not block:
                            break
                        copy.write(block)
            conn.commit()
            print(
                f"[{time.strftime('%H:%M:%S')}] Loaded musicbrainz.{table_name} in {(time.time() - start_t):.1f}s.",
                flush=True,
            )


def parse_args():
    parser = argparse.ArgumentParser(description="Download and load MusicBrainz relational staging database")
    parser.add_argument(
        "--dump-tag",
        type=str,
        default=os.getenv("MB_DUMP_TAG", ""),
        help="Specific MetaBrainz fullexport tag (e.g. 20261125-001614). Defaults to LATEST.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        default=False,
        help="Skip downloading and use existing tar.bz2 archives in MB_DOWNLOAD_DIR",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dump_tag = args.dump_tag.strip() or get_latest_dump_tag()
    print(f"Using MusicBrainz fullexport tag: {dump_tag}", flush=True)

    core_tar = os.path.join(DOWNLOAD_DIR, dump_tag, "mbdump.tar.bz2")
    derived_tar = os.path.join(DOWNLOAD_DIR, dump_tag, "mbdump-derived.tar.bz2")

    if not args.skip_download:
        download_file_if_needed(f"{MB_BASE_URL}/{dump_tag}/mbdump.tar.bz2", core_tar)
        download_file_if_needed(f"{MB_BASE_URL}/{dump_tag}/mbdump-derived.tar.bz2", derived_tar)

    print(f"[{time.strftime('%H:%M:%S')}] Initializing musicbrainz schema in PostgreSQL...", flush=True)
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(MB_DDL)

    with psycopg.connect(POSTGRES_DSN, autocommit=False) as conn:
        stream_tar_to_postgres(core_tar, CORE_TABLES, conn)
        stream_tar_to_postgres(derived_tar, DERIVED_TABLES, conn)

    print(f"[{time.strftime('%H:%M:%S')}] Building primary keys, indexes, and helper views...", flush=True)
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(MB_INDEXES_AND_VIEWS)

    print("MusicBrainz staging database setup complete.", flush=True)


if __name__ == "__main__":
    main()