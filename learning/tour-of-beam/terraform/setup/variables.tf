variable "project_id" {
  description = "The GCP Project ID where ToB Cloud Functions will be created"
}

variable "service_account_id" {
  description = "Service account ID"
  default     = "tour-of-beam-sa"
}

