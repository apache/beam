resource "google_artifact_registry_repository" "playground_repo" {
  provider = google-beta

  project = "${var.project_id}"
  location = "${var.repository_location}"
  repository_id = "${var.repository_id}"
  description = "Playground docker repository"
  format = "DOCKER"
}
