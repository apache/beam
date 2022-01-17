# need enable api
#  https://console.developers.google.com/apis/api/iam.googleapis.com/overview?project=750903720791
resource "google_service_account" "account" {
  project            = "${var.project_id}"
  account_id   = "${var.service_account}"
  display_name = "${var.service_account}"
}


output "playground_service_accoun_id" {
  value = "${google_service_account.account.account_id}"
}


resource "google_project_iam_binding" "account_iam_binding" {
  for_each = toset([
  "roles/dataflow.admin",
  "roles/compute.storageAdmin",
  "roles/iam.serviceAccountTokenCreator"
  ])
  role     = each.value
  project = "${var.project_id}"
  members = [
  "serviceAccount:${google_service_account.account.email}"
  ]
}





