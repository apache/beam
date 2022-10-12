output "cloud_function_trigger_url" {
  value = google_cloudfunctions_function.cloud_function.https_trigger_url
}