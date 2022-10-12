variable "project_id" {
  description = "The GCP Project ID where ToB Cloud Functions will be created"
}

variable "region" {
  description = "Region of App"
}

variable "cloud_function_name" {
  description = "Name of cloud function"
}

variable "service_account_id" {
  description = "Service account ID"
  default     = "tour-of-beam-sa"
}