terraform {
  # this describe buket for save state playground cloud
  backend "gcs" {
  }

  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.0.0"
    }
  }
}

provider "google" {
  region = var.region
  project = var.project_id
  // TODO may need to run module.setup first independent of this solution and add the terraform service account as a variable
  // This allows us to use a service account to provision resources without downloading or storing service account keys
}
