resource "google_project_service" "api_enable" {
  project = var.project_id
  for_each = toset(var.services)
  service = each.value
  timeouts {
    create = "30m"
    update = "40m"
  }

  disable_dependent_services = true
}
