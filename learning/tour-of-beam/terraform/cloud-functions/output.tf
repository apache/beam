output "cloud-function-trigger-url" {
  value = google_cloudfunctions_function.cloud_function.https_trigger_url
}

output "cloud-function-name" {
  value = google_cloudfunctions_function.cloud_function.name
}

output "cloud-function-region" {
  value = google_cloudfunctions_function.cloud_function.region
}