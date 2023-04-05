resource "google_artifact_registry_repository" "default" {
  format        = "Docker"
  repository_id = var.resource_name_prefix
  location      = var.region
}
