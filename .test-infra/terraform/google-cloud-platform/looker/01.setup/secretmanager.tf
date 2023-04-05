// Create Secret Manager secret to store the Looker License key.
resource "google_secret_manager_secret" "looker_license_key" {
  depends_on = [google_project_service.required]
  secret_id  = "looker-license-key"
  replication {
    automatic = true
  }
}
