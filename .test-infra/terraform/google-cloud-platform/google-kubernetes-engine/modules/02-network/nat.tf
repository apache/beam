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

// For security, provision the Kubernetes node using private IP addresses.
// The recommendation in this context is to additionally provision
// a Cloud NAT and Router so that private IP only compute engine instances
// (created as a result of Google Kubernetes Engine node pool) can access
// resources outside the Google Virtual Private Cloud
// See https://cloud.google.com/nat/docs/overview
resource "google_compute_router_nat" "default" {
  name                               = "${google_compute_router.default.name}-nat"
  nat_ip_allocate_option             = "AUTO_ONLY"
  region                             = google_compute_router.default.region
  router                             = google_compute_router.default.name
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}

// Required by the Cloud NAT.
// See https://cloud.google.com/network-connectivity/docs/router.
resource "google_compute_router" "default" {
  name    = "${google_compute_subnetwork.default.name}-${var.region}-router"
  network = google_compute_subnetwork.default.network
  region  = google_compute_subnetwork.default.region
}