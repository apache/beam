// Create secret to store the Looker License key.
resource "google_secret_manager_secret" "looker_license_key" {
  depends_on = [google_project_service.required]
  secret_id  = "looker-license-key"
  replication {
    automatic = true
  }
}

// Create secret to store the gcm encryption key.
resource "google_secret_manager_secret" "gcm_key" {
  depends_on = [google_project_service.required]
  secret_id = "gcm-key"
  replication {
    automatic = true
  }
}

// Create secret to store the Looker database password
resource "google_secret_manager_secret" "looker_db_password" {
  depends_on = [google_project_service.required]
  secret_id = "looker-db-password"
  replication {
    automatic = true
  }
}