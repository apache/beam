output "function-bucket-id" {
  value = google_storage_bucket.function_bucket.id
}

output "function-bucket-name" {
  value = google_storage_bucket.function_bucket.name
}

output "function-bucket-object" {
  value = google_storage_bucket_object.zip.name
}

output "function-bucket-location" {
  value = google_storage_bucket.function_bucket.location
}