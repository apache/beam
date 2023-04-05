output "looker_jars_glob" {
  value = "gs://${google_storage_bucket.default.name}/${var.looker_version}/*.jar"
}
