resource "google_app_engine_application" "app_playground" {
  project     = var.project_id
  location_id = var.region
  database_type = "CLOUD_DATASTORE_COMPATIBILITY"
}

resource "google_project_service" "firestore" {
  project = var.project_id
  service = "firestore.googleapis.com"
  disable_dependent_services = true
  depends_on = [
    google_app_engine_application.app_playground
    ]
}

resource "google_app_engine_flexible_app_version" "default_app" {
  depends_on = [
    google_app_engine_application.app_playground
    ]
  count      = var.create_default_service ? 1 : 0
  service    = "default"
  version_id = "mlflow-default"
  runtime    = "custom"
  project    = var.project_id

  deployment {
    container {
      image = "gcr.io/cloudrun/hello"
    }
  }

  liveness_check {
    path = "/"
  }

  readiness_check {
    path = "/"
  }

  automatic_scaling {
    cool_down_period    = "120s"
    min_total_instances = 1
    max_total_instances = 1
    cpu_utilization {
      target_utilization = 0.5
    }
  }

  delete_service_on_destroy = false
  noop_on_destroy           = true

}
