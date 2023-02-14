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

# Create firebase project
resource "google_firebase_project" "tourofbeam_firebase_project" {
  provider = google-beta
  project = var.project_id
}

resource "google_firebase_web_app" "tob_firebase_webapp" {
  provider     = google-beta
  project = var.project_id
  display_name = "${var.project_id} Firebase Web App"
}

resource "google_firebase_hosting_site" "tob_firebase_hostingsite" {
  provider = google-beta
  project  = var.project_id
  site_id = "${var.project_id}-tourofbeam-web"
  app_id = google_firebase_web_app.tob_firebase_webapp.app_id
  depends_on = [google_firebase_web_app.tob_firebase_webapp]
}

resource "google_storage_bucket" "firebase_storage" {
  name          = var.firebase_storage_bucket_name
  location      = var.region
  storage_class = "STANDARD"
}

resource "google_firebase_storage_bucket" "default" {
  provider  = google-beta
  project   = var.project_id
  bucket_id = google_storage_bucket.firebase_storage.id
}