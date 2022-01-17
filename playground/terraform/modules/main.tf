provider "google" {
  region      = "us-central"
}
provider "google-beta" {
  region      = "us-central"
}


module "vpc" {
  source = "./vpc"
  project_id = "${var.project_id}"
}

module "buckets" {
  source = "./buckets"
  project_id = "${var.project_id}"
  terraform_bucket_name = "${var.terraform_bucket_name}"
  examples_bucket_name = "${var.examples_bucket_name}"
}

module "artifact_registry" {
  source = "./artifact_registry"
  project_id = "${var.project_id}"
  repository_id = "${var.repository_id}"
  depends_on = [module.buckets]
}

module "memorystore" {
  source = "./memorystore"
  project_id = "${var.project_id}"
  beam_playground_terraform = "${var.terraform_bucket_name}"
  depends_on = [module.artifact_registry]
}

module "gke" {
  source = "./gke"
  project_id = "${var.project_id}"
  service_account  = google_service_account.account.account_id
  depends_on = [module.artifact_registry,module.memorystore, google_service_account.account]
}


