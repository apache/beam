variable "project_id" {
  type = string
  description = "The ID of the Google Cloud project within which resources are provisioned"
}
variable "cloudbuild_service_account_id" {
  type = string
  description = "The ID of the cloud build service account responsible for provisioning Google Cloud resources"
}