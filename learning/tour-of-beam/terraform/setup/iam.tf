resource "google_service_account" "sa_cloud_function" {
  account_id   = var.service_account_id
  display_name = "Service Account to run Cloud Functions"
  project      = var.project_id
}

resource "google_project_iam_member" "terraform_service_account_roles" {
  for_each = toset([
    "roles/cloudfunctions.developer", "roles/artifactregistry.reader", "roles/storage.objectViewer", "roles/storage.objectCreator",
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.sa_cloud_function.email}"
  project = var.project_id
}