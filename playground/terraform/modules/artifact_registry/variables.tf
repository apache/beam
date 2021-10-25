variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "repository_location" {
  description = "Location of Artifact Registry"
  default     = "us-central1"
}

variable "repository_id" {
  description = "ID of Artifact Registry"
  default     = "playground-repository"
}


