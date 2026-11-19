variable "project" {
  description = "Project ID"
  default     = "spotify-data-pipeline-513809"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "spotify_data_raw"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "spotify-datalake-spotify-data-pipeline-513809"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}