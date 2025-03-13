# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


resource "google_app_engine_application" "app_playground" {
  count         = var.skip_appengine_deploy ? 0 : 1
  project     = var.project_id
  location_id = var.region == "us-central1" ? var.location_id_us : (var.region == "europe-west1") ? var.location_id_eu : var.region
  database_type = "CLOUD_DATASTORE_COMPATIBILITY"
}

resource "google_project_service" "firestore" {
  count      = var.skip_appengine_deploy ? 0 : 1
  project = var.project_id
  service = "firestore.googleapis.com"
  disable_dependent_services = true
  depends_on = [
    google_app_engine_application.app_playground
    ]
}
