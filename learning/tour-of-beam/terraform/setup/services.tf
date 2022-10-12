# Enable API for Cloud Build
resource "google_project_service" "cloud_build" {
  project = var.project_id
  service = "cloudbuild.googleapis.com"
}

# Enable API for Cloud Function
resource "google_project_service" "cloud_function" {
  project = var.project_id
  service = "cloudfunctions.googleapis.com"
}

# Enable API for Resource Manager
resource "google_project_service" "resource_manager" {
  project = var.project_id
  service = "cloudresourcemanager.googleapis.com"
}

# Enable API for IAM
resource "google_project_service" "iam" {
  project = var.project_id
  service = "iam.googleapis.com"
}