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

resource "google_container_cluster" "playground-gke" {
  name                       = var.name
  project                    = var.project_id
  location                   = var.location
  initial_node_count         = var.init_min_count
  network                    = var.network
  subnetwork                 = var.subnetwork
  remove_default_node_pool = true

  private_cluster_config {
   enable_private_nodes = true
   enable_private_endpoint = false
   master_ipv4_cidr_block = var.control_plane_cidr
}
}
resource "google_container_node_pool" "playground-node-pool" {
  name       = "playground-node-pool"
  cluster    = google_container_cluster.playground-gke.name
  location   = google_container_cluster.playground-gke.location
  autoscaling {
    min_node_count = var.min_count
    max_node_count = var.max_count
   }
  management {
    auto_repair  = "true"
    auto_upgrade = "true"
   }
  node_config {
    machine_type    = var.machine_type
    service_account = var.service_account_email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    labels = {
      component = "beam-playground"
    }
    tags = ["beam-playground"]
   }
}