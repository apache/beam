output "examples-bucket-id" {
  value = "${google_storage_bucket.examples_bucket.id}"
}

output "examples-bucket-name" {
  value = "${google_storage_bucket.examples_bucket.name}"
}

output "examples-bucket-project" {
  value = "${google_storage_bucket.examples_bucket.project}"
}

output "examples-bucket-location" {
  value = "${google_storage_bucket.examples_bucket.location}"
}

output "terraform-bucket-id" {
  value = "${google_storage_bucket.terraform_bucket.id}"
}

output "terraform-bucket-name" {
  value = "${google_storage_bucket.terraform_bucket.name}"
}

output "terraform-bucket-project" {
  value = "${google_storage_bucket.terraform_bucket.project}"
}

output "terraform-bucket-location" {
  value = "${google_storage_bucket.terraform_bucket.location}"
}
