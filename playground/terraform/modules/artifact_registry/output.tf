output "registry_name" {
  value = "${google_artifact_registry_repository.playground_repo.name}"
}

output "regirsty_id" {
  value = "${google_artifact_registry_repository.playground_repo.id}"
}

output "location" {
  value = "${google_artifact_registry_repository.playground_repo.location}"
}
