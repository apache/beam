module "cloud-functions" {
  source = "../cloud-functions"
}

resource "google_service_account" "sa_cloud_function" {
  account_id   = var.service_account_id
  display_name = "Service Account to run Cloud Functions"
  project      = var.project_id
}

resource "google_project_iam_member" "terraform_service_account_roles" {
  for_each = toset([
    "roles/cloudfunctions.developer", "roles/storage.objectViewer", "roles/storage.objectCreator",
  ])
  role    = each.key
  member  = "serviceAccount:${google_service_account.sa_cloud_function.email}"
  project = var.project_id
}

resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = var.project_id
  region         = module.cloud-functions.cloud-function-region
  cloud_function = module.cloud-functions.cloud-function-name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}