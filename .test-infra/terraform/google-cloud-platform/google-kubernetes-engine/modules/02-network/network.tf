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

// Provision virtual custom 02-network
resource "google_compute_network" "default" {
  depends_on              = [google_project_service.compute]
  name                    = var.network_base_name
  auto_create_subnetworks = false
}

// Provision subnetwork of the virtual custom 02-network
resource "google_compute_subnetwork" "default" {
  name                     = google_compute_network.default.name
  ip_cidr_range            = var.subnetwork_cidr_range
  network                  = google_compute_network.default.name
  private_ip_google_access = true
  region                   = var.region
}

// Provision firewall rule for internal 02-network traffic only
resource "google_compute_firewall" "default" {
  name    = "allow-${google_compute_network.default.name}-internal"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
  }

  source_service_accounts = [
    var.kubernetes_node_service_account.email
  ]
}

// Provision firewall rule for SSH ingress from identity aware proxy
// See https://cloud.google.com/iap/docs/using-tcp-forwarding#create-firewall-rule
resource "google_compute_firewall" "iap" {
  name    = "allow-${google_compute_network.default.name}-ssh-ingress-from-iap"
  network = google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = [
      22,
    ]
  }

  source_ranges = [
    "35.235.240.0/20",
  ]
}
