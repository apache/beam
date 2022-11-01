provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}


resource "google_storage_bucket" "remote-gradle-cache-storage" {
  name          = var.bucket-name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition {
      age = 15
    }
    action {
      type = "Delete"
    }
  }
}
