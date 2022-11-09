variable "project" {
  type = string
  description = "The ID of the Google Cloud project within which resources are provisioned"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
  default     = "us-central1"
}

variable "cloudbuild_service_account_id" {
  type = string
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
  default = "terraform-cloudbuild"
}