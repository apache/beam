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
  network                    = data.google_compute_network.actions-runner-network.id
  subnetwork                 = google_compute_subnetwork.actions-runner-subnetwork.id
  remove_default_node_pool   = true

}
resource "google_container_node_pool" "main-actions-runner-pool" {
  name       = "main-pool"
  cluster    = google_container_cluster.actions-runner-gke.name
  location   = google_container_cluster.actions-runner-gke.location
  autoscaling {
    min_node_count = var.main_runner.min_node_count
    max_node_count = var.main_runner.max_node_count
   }
   initial_node_count = var.main_runner.min_node_count
  management {
    auto_repair  = "true"
    auto_upgrade = "true"
   }
  node_config {
    disk_size_gb = var.main_runner.disk_size_gb
    machine_type = var.main_runner.machine_type
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    service_account = data.google_service_account.service_account.email
    tags = ["actions-runner-pool"]
   }
}

resource "google_container_node_pool" "additional_runner_pools" {
  for_each = {
    for index, runner_pool in var.additional_runner_pools : runner_pool.name => runner_pool
  }

  name       = each.value.name
  cluster    = google_container_cluster.actions-runner-gke.name
  location   = google_container_cluster.actions-runner-gke.location
  autoscaling {
    min_node_count = each.value.min_node_count
    max_node_count = each.value.max_node_count
   }
   initial_node_count = each.value.min_node_count
  management {
    auto_repair  = "true"
    auto_upgrade = "true"
   }
  node_config {
    disk_size_gb = each.value.disk_size_gb
    machine_type    = each.value.machine_type
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    service_account = data.google_service_account.service_account.email
    tags = ["actions-runner-pool"]
    labels = {
      "runner-pool" = each.value.name
    }
   
    dynamic "taint" {
      for_each = each.value.enable_taint == true ? [1] : []
      content {
        key    = "runner-pool"
        value  = each.value.name
        effect = "NO_SCHEDULE"
        }
      }
    }
  }


resource "google_compute_global_address" "actions-runner-ip" {
  count = var.deploy_webhook == "true" && var.existing_ip_name == "" ? 1 : 0
  name  = "${var.environment}-actions-runner-ip"
}

data "google_compute_global_address" "actions-runner-ip" {
  count = var.deploy_webhook == "true" ? 1 : 0
  name  = var.existing_ip_name == "" ? google_compute_global_address.actions-runner-ip[0].name : var.existing_ip_name
}

data google_service_account "service_account" {
  account_id = var.service_account_id
}