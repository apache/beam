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

// Private IP-only Compute Engine instances require a Cloud NAT and router
// to access public internet resources.
resource "google_compute_router" "default" {
  name    = "${data.google_compute_network.default.name}-${var.region}-router"
  network = data.google_compute_network.default.id
  region  = var.region
}

// Private IP-only Compute Engine instances require a Cloud NAT and router
// to access public internet resources.
resource "google_compute_router_nat" "default" {
  name                               = "${data.google_compute_network.default.name}-${var.region}-router-nat"
  nat_ip_allocate_option             = "AUTO_ONLY"
  router                             = google_compute_router.default.name
  region                             = google_compute_router.default.region
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  log_config {
    enable = true
    filter = "ERRORS_ONLY"
  }
}
