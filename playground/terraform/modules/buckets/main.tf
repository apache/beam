resource "google_storage_bucket" "examples_bucket" {
  name          = "${var.examples_bucket_name}"
  location      = "${var.examples_bucket_location}"
  project       = "${var.project_id}"
  storage_class = "${var.examples_storage_class}"
}

resource "google_storage_bucket" "terraform_bucket" {
  name          = "${var.terraform_bucket_name}"
  location      = "${var.terraform_bucket_location}"
  project       = "${var.project_id}"
  storage_class = "${var.terraform_storage_class}"
}
