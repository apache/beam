variable "project_id" {
 description = "project_id"
}

variable "services" {
 description = "Enable necessary APIs in GCP"
 default = ["cloudresourcemanager.googleapis.com","iam.googleapis.com","compute.googleapis.com","appengine.googleapis.com"]
}
