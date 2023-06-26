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
resource "google_container_cluster" "actions-runner-gke" {
  name                       = "${var.environment}-actions-runner-gke"
  project                    = var.project_id
  location                   = var.zone
  initial_node_count         = 1
  network                    = google_compute_network.actions-runner-network.id
  subnetwork                 = google_compute_subnetwork.actions-runner-subnetwork.id
  remove_default_node_pool = true

}
resource "google_container_node_pool" "actions-runner-pool" {
  name       = "main-pool"
  cluster    = google_container_cluster.actions-runner-gke.name
  location   = google_container_cluster.actions-runner-gke.location
  autoscaling {
    min_node_count = var.min_main_node_count
    max_node_count = var.max_main_node_count
   }
  management {
    auto_repair  = "true"
    auto_upgrade = "true"
   }
  node_config {
    machine_type    = var.machine_type
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    tags = ["actions-runner-pool"]
   }
}
resource "google_compute_global_address" "actions-runner-ip" {
  name      = "${var.environment}-actions-runner-ip"
}