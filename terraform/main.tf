terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project     = var.project
  region      = var.region
}

# This data source gets a temporary token for the service account
data "google_service_account_access_token" "default" {
provider               = google
target_service_account = "spotify-pipeline-sa@spotify-data-pipeline-513809.iam.gserviceaccount.com"
scopes                 = ["https://www.googleapis.com/auth/cloud-platform"]
lifetime               = "3600s"
}

# This second provider block uses that temporary token and does the real work
provider "google" {
alias        = "impersonated"
access_token = data.google_service_account_access_token.default.access_token
project      = var.project
region       = var.region
}

# 1. Data Lake: Google Cloud Storage Bucket
resource "google_storage_bucket" "data-lake-bucket" {
  provider      = google.impersonated
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class

  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# 2. Data Warehouse: BigQuery Dataset
resource "google_bigquery_dataset" "raw_dataset" {
  provider   = google.impersonated
  dataset_id = var.bq_dataset_name
  location   = var.location
  
  delete_contents_on_destroy = true
}