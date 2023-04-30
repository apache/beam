# GCP API Services to be enabled
resource "google_project_service" "required_services" {
  for_each  = toset([
    "cloudresourcemanager",
    "iam",
    "cloudbuild",
    "cloudfunctions"
  ])
  service   = "${each.key}.googleapis.com"
  disable_on_destroy = false
}
