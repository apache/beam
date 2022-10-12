output "cloud-function-trigger-url" {
  value = google_cloudfunctions_function.cloud_function.https_trigger_url
}