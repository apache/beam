resource "google_secret_manager_secret" "secret_webhook_cloudbuild_trigger_cicd" {
  secret_id = var.webhook_trigger_secret_id

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret" "secret_gh_pat_cloudbuild" {
  secret_id = var.gh_pat_secret

  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "secret_webhook_cloudbuild_trigger_cicd_data" {
  secret = google_secret_manager_secret.secret_webhook_cloudbuild_trigger_cicd.id
  secret_data = var.data_for_cicd_webhook_secret
}

resource "google_secret_manager_secret_version" "secret_gh_pat_cloudbuild_data" {
  secret = google_secret_manager_secret.secret_gh_pat_cloudbuild.id
  secret_data = var.data_for_github_pat_secret
}
