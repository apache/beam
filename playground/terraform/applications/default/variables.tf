variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "location" {
  description = "Location of App"
  default     = "us-central"
}

variable "create_default_service" {
  description = "Whether or not to create a default app engine service"
  type        = bool
  default = false
}