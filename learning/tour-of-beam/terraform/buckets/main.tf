#Generates archive of source code
data "archive_file" "source" {
  type        = "zip"
  source_dir  = "../../backend"
  output_path = "/tmp/backend.zip"
}

resource "google_storage_bucket" "function_bucket" {
  name          = var.name
  location      = var.location
  project       = var.project_id
  storage_class = var.storage_class
}

resource "google_storage_bucket_object" "zip" {
  # Use an MD5 here. If there's no changes to the source code, this won't change either.
  # We can avoid unnecessary redeployments by validating the code is unchanged, and forcing
  # a redeployment when it has!
  name         = "${data.archive_file.source.output_md5}.zip"
  bucket       = google_storage_bucket.function_bucket.name
  source       = data.archive_file.source.output_path
  content_type = "application/zip"
}

resource "google_storage_bucket_access_control" "public_rule" {
  bucket = google_storage_bucket.function_bucket.name
  role   = "READER"
  entity = "allUsers"
}
