variable "examples_bucket_name" {
  description = "Name of Bucket to Store Playground Examples"
  default     = "playground-examples"
}

variable "examples_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "examples_storage_class" {
  description = "Examples Bucket Storage Class"
  default     = "STANDARD"
}

variable "terraform_bucket_name" {
  description = "Name of Bucket to Store Terraform States"
  default     = "terraform-examples"
}

variable "terraform_bucket_location" {
  description = "Location of Playground Examples Bucket"
  default     = "US"
}

variable "terraform_storage_class" {
  description = "Terrafomr Bucket Storage Class"
  default     = "STANDARD"
}
