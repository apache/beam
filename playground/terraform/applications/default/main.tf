#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

resource "google_app_engine_application" "app_playground" {
  project     = var.project_id
  location_id = var.location
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

