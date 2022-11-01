
variable "location" {
  type        = string
  description = "GCP Location for bucket"
}

variable "region" {
  type        = string
  description = "GCP Region"
}
variable "zone" {
  type        = string
  description = "GCP Zone"
}

variable "project" {
  type        = string
  description = "GCP Project id"
}

variable "bucket-name" {
  type        = string
  description = "Name of the GCP bucket for remote gradle cache storage"
}
