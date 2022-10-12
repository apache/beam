#COMMON

variable "project_id" {
  description = "The GCP Project ID where ToB Cloud Functions will be created"
}

variable "region" {
  description = "Infrastructure Region"
}

#IAM

variable "service_account_id" {
  description = "Service account ID"
  default     = "tour-of-beam-sa"
}

#Buckets

variable "function_bucket_name" {
  description = "Name of Bucket to Store Cloud Functions"
  default     = "tour-of-beam-cloudfunction-bucket"
}

variable "function_bucket_location" {
  description = "Cloud Functions Bucket Region"
  default     = "US"
}

variable "function_bucket_storage_class" {
  description = "Functions Bucket Storage Class"
  default     = "STANDARD"
}