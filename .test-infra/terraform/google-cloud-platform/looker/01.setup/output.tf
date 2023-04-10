output "artifact_registry_url" {
  value = "${google_artifact_registry_repository.default.location}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.default.repository_id}"
}

output "looker_jars_bucket" {
  value = google_storage_bucket.looker_jars.name
}
