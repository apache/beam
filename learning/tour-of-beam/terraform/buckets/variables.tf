variable "name" {
  description = "Name of Bucket to Store Cloud Functions"
  default     = "tour-of-beam-cloudfunction-bucket"
}

variable "location" {
  description = "Cloud Functions Bucket Region"
  default     = "US"
}

variable "project_id" {
  description = "The GCP Project ID where ToB Cloud Functions will be created"
}

variable "storage_class" {
  description = "Functions Bucket Storage Class"
  default     = "STANDARD"
}