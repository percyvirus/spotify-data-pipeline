import argparse
import os
import shutil

from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi
from tqdm import tqdm

# --- Configuration ---
KAGGLE_DATASET = "lordpatil/spotify-metadata-by-annas-archive"
DATASET_FOLDER = KAGGLE_DATASET.split('/')[-1]
LOCAL_DOWNLOAD_PATH = "/tmp/spotify_dataset"
GCS_BUCKET_NAME = os.getenv(
    "GCS_BUCKET_NAME",
    "spotify-datalake-spotify-data-pipeline-513809"
)
GCP_PROJECT_ID = os.getenv(
    "GCP_PROJECT_ID",
    "spotify-data-pipeline-513809"
)

ALLOWED_EXTENSIONS = (".parquet", ".json")


def is_allowed_file(filename: str) -> bool:
    """Check if a file has an allowed extension and is not an OS metadata file."""
    if filename.startswith((".", "._")):
        return False

    if filename in [".DS_Store", "Thumbs.db"]:
        return False

    return filename.lower().endswith(ALLOWED_EXTENSIONS)


def get_gcs_state(
    client: storage.Client,
    bucket_name: str,
    prefix: str = "raw/"
) -> dict[str, int]:
    """Return {blob_name: size_in_bytes} for files currently in GCS."""

    state = {}
    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs(prefix=prefix):
        if is_allowed_file(blob.name):
            state[blob.name] = blob.size

    return state


def get_kaggle_state(api: KaggleApi, dataset: str) -> dict[str, int]:
    """Fetch file metadata from Kaggle without downloading the dataset."""

    state = {}

    files = api.dataset_list_files(dataset)
    file_list = getattr(files, "files", files)

    for f in file_list:
        # Support both dictionary and object responses
        if isinstance(f, dict):
            name = f.get("name", "")
            size = (
                f.get("total_bytes")
                or f.get("totalBytes")
                or f.get("size")
            )
        else:
            name = getattr(f, "name", "")
            size = getattr(f, "total_bytes", None)

            if size is None:
                size = getattr(f, "totalBytes", None)

            if size is None:
                size = getattr(f, "size", None)

        name = str(name)

        if not is_allowed_file(name):
            continue

        if size is None:
            raise ValueError(f"Could not determine size for Kaggle file: {name}")

        state[f"raw/{DATASET_FOLDER}/{name}"] = int(size)

    if not state:
        raise RuntimeError(
            f"Kaggle returned no valid files for dataset {dataset}"
        )

    return state


def get_local_files(base_path: str) -> list[dict]:
    """Scan directory and return metadata for valid local files."""

    files_metadata = []

    for root, _, files in os.walk(base_path):
        for file in files:

            if not is_allowed_file(file):
                continue

            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, base_path)
            destination_blob = f"raw/{DATASET_FOLDER}/{rel_path}"
            size = os.path.getsize(full_path)

            files_metadata.append({
                "local_path": full_path,
                "blob_name": destination_blob,
                "size": size,
                "filename": file
            })

    return files_metadata


def upload_to_gcs(
    client: storage.Client,
    bucket_name: str,
    source_path: str,
    blob_name: str
):
    """Upload a single file to GCS."""

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(source_path)


def main():

    parser = argparse.ArgumentParser(
        description="Ingest Spotify metadata to GCS Data Lake"
    )

    parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force re-download and re-upload the entire dataset"
    )

    args = parser.parse_args()

    # Step 1: Authenticate APIs
    print("Authenticating APIs (GCP & Kaggle)...")

    gcs_client = storage.Client(project=GCP_PROJECT_ID)

    kaggle_api = KaggleApi()
    kaggle_api.authenticate()

    # Step 2: Fetch metadata from Kaggle and GCS
    print("\nFetching metadata from Kaggle and GCS...")

    kaggle_state = get_kaggle_state(kaggle_api, KAGGLE_DATASET)
    gcs_state = get_gcs_state(gcs_client, GCS_BUCKET_NAME)

    print(f"Kaggle files: {len(kaggle_state)}")
    print(f"GCS files: {len(gcs_state)}")

    # Step 3: Compare both states
    missing_files = []
    size_mismatches = []

    for blob_name, kaggle_size in kaggle_state.items():

        if blob_name not in gcs_state:
            missing_files.append(blob_name)

        elif gcs_state[blob_name] != kaggle_size:
            size_mismatches.append({
                "file": blob_name,
                "kaggle_size": kaggle_size,
                "gcs_size": gcs_state[blob_name]
            })

    # Step 4: Decide whether ingestion is necessary
    needs_sync = bool(missing_files or size_mismatches)

    if args.force:
        print("\nFORCE mode enabled.")
        print("The entire dataset will be downloaded and uploaded.")

    elif not needs_sync:
        print("\nSUCCESS: Data Lake is already synchronized!")
        print(f"All {len(kaggle_state)} files match.")
        print("No download or upload required.")
        return

    else:
        print("\nData Lake is NOT fully synchronized.")

        if missing_files:
            print(f"\nMissing files in GCS: {len(missing_files)}")
            for filename in missing_files:
                print(f"  - {filename}")

        if size_mismatches:
            print(f"\nFiles with different sizes: {len(size_mismatches)}")
            for item in size_mismatches:
                print(
                    f"  - {item['file']}: "
                    f"Kaggle={item['kaggle_size']} bytes, "
                    f"GCS={item['gcs_size']} bytes"
                )

    # Step 5: Download dataset from Kaggle
    print("\nPreparing local download directory...")

    # Remove previous downloads to avoid using stale local files
    if os.path.exists(LOCAL_DOWNLOAD_PATH):
        shutil.rmtree(LOCAL_DOWNLOAD_PATH)

    os.makedirs(LOCAL_DOWNLOAD_PATH, exist_ok=True)

    print(f"Downloading dataset from Kaggle to {LOCAL_DOWNLOAD_PATH}...")

    kaggle_api.dataset_download_files(
        KAGGLE_DATASET,
        path=LOCAL_DOWNLOAD_PATH,
        force=True,
        quiet=False,
        unzip=True
    )

    # Step 6: Validate downloaded files
    print("\nValidating downloaded files...")

    local_files = get_local_files(LOCAL_DOWNLOAD_PATH)

    if not local_files:
        raise RuntimeError("No valid files found after Kaggle download.")

    local_state = {
        f["blob_name"]: f["size"]
        for f in local_files
    }

    for blob_name, expected_size in kaggle_state.items():

        if blob_name not in local_state:
            raise RuntimeError(
                f"Downloaded file is missing: {blob_name}"
            )

        if local_state[blob_name] != expected_size:
            raise RuntimeError(
                f"Downloaded file has unexpected size: {blob_name}. "
                f"Expected={expected_size}, "
                f"Downloaded={local_state[blob_name]}"
            )

    print(f"Validated {len(local_files)} local files.")

    # Step 7: Determine which files need uploading
    if args.force:
        files_to_upload = local_files

    else:
        files_to_upload = [
            f for f in local_files
            if (
                f["blob_name"] not in gcs_state
                or gcs_state[f["blob_name"]] != f["size"]
            )
        ]

    if not files_to_upload:
        print("\nNothing to upload.")
        return

    # Step 8: Upload files to GCS
    print(
        f"\nUploading {len(files_to_upload)} file(s) "
        f"to gs://{GCS_BUCKET_NAME}/raw/..."
    )

    with tqdm(
        files_to_upload,
        desc="Uploading to GCS",
        unit="file"
    ) as pbar:

        for f in pbar:
            pbar.set_postfix_str(
                f"Uploading: {f['filename'][:25]}..."
            )

            upload_to_gcs(
                gcs_client,
                GCS_BUCKET_NAME,
                f["local_path"],
                f["blob_name"]
            )

    print("\nIngestion completed successfully!")
    print(f"Downloaded: {len(local_files)} file(s)")
    print(f"Uploaded/Updated: {len(files_to_upload)} file(s)")


if __name__ == "__main__":
    main()