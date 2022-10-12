module "setup" {
  source = "../setup/iam.tf"
}

module "buckets" {
  source = "../buckets/main.tf"
}

resource "google_cloudfunctions_function" "cloud_function" {
  name                  = var.cloud_function_name
  runtime               = go116
  region                = var.region
  service_account_email = module.setup.google_service_account.sa_cloud_function.email
  ingress_settings      = "ALLOW_ALL"
  # Get the source code of the cloud function as a Zip compression
  source_archive_bucket = google_storage_bucket.
  source_archive_object = google_storage_bucket_object.zip.name

  trigger_http = true
  # Name of the function that will be executed when the Google Cloud Function is triggered (def hello_gcs)
  entry_point           = var.code_function_name

}