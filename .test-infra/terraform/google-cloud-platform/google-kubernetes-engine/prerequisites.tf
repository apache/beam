/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

resource "google_project_service" "required" {
  for_each = toset([
    "container",
    "iam",
  ])
  service            = "${each.key}.googleapis.com"
  disable_on_destroy = false
}

// Query the VPC network to make sure it exists.
data "google_compute_network" "default" {
  depends_on = [google_project_service.required]
  name       = var.network
}

// Query the VPC subnetwork to make sure it exists in the region specified.
data "google_compute_subnetwork" "default" {
  depends_on = [google_project_service.required]
  name       = var.subnetwork
  region     = var.region
}

// Query the Service Account.
data "google_service_account" "default" {
  depends_on = [google_project_service.required]
  account_id = var.service_account_id
}
