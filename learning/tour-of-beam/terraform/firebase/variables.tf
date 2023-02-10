variable "project_id" {
  type  = string
}

variable "firebase_webapp_id" {
  type  = string
}

variable "firebase_region" {
  description = <<EOF
The ID of the GCP resource location for the Firebase Project.
For Firebase region details see:
https://firebase.google.com/docs/projects/locations#location-r
  EOF
}