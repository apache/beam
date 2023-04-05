resource "random_string" "default" {
  length  = 8
  upper   = false
  special = false
}

resource "google_storage_bucket" "default" {
  name                        = "${var.storage_bucket_prefix}-${random_string.default.result}"
  location                    = var.storage_bucket_location
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "looker_jar" {
  depends_on   = [null_resource.looker_jar]
  bucket       = google_storage_bucket.default.name
  name         = "${var.looker_version}/looker.jar"
  content_type = "application/java-archive"
  source       = "${local.temporary.looker}/${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.version_text])}"
}

resource "google_storage_bucket_object" "looker_dependencies_jar" {
  depends_on   = [null_resource.looker_dependencies_jar]
  bucket       = google_storage_bucket.default.name
  name         = "${var.looker_version}/looker-dependencies.jar"
  content_type = "application/java-archive"
  source       = "${local.temporary.looker}/${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.depDisplayFile])}"
}

resource "google_storage_bucket_object" "mx_prometheus_javaagent" {
  depends_on   = [null_resource.prometheus_jar]
  bucket       = google_storage_bucket.default.name
  name         = "${var.looker_version}/${basename(local.prometheus.target_path)}"
  content_type = "application/java-archive"
  source       = local.prometheus.target_path
}