variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "examples_bucket_name" {
  description = "Name of Bucket to Store Playground Examples"
  default     = "playground-examples_1"
}

variable "terraform_bucket_name" {
  description = "Name of Bucket to Store Terraform States"
  default     = "playground_terraform_1"
}

variable "repository_id" {
  description = "ID of Artifact Registry"
  default     = "playground-repository"
}

variable "service_account" {
  description = "Service account email"
  default = "service-account-playground"
}