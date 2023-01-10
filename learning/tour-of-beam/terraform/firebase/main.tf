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

resource "google_firebase_project" "tob_firebase_project" {
  provider = google-beta
}

resource "google_firebase_project_location" "tob_firebase_project_location" {
  provider = google-beta
  location_id = us-central1
}

resource "google_firebase_web_app" "tob_firebase_firebase_webapp" {
  provider     = google-beta
  display_name = "Tour Of Beam Web App"
}

resource "google_firebase_hosting_site" "tob_firebase_hosting" {
  provider = google-beta
  site_id  = var.hosting_site_id
  app_id   = google_firebase_web_app.tob_firebase_firebase_webapp.app_id
}