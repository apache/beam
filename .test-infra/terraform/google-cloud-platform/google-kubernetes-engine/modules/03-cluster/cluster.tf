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

// Provisions regional private Google Kubernetes Engine cluster
resource "google_container_cluster" "default" {
  depends_on       = [google_project_service.container]
  name             = var.cluster_name
  location         = var.region
  enable_autopilot = true
  network          = var.network.id
  subnetwork       = var.subnetwork.id
  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = var.kubernetes_node_service_account.email
      oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }
  master_authorized_networks_config {}
  private_cluster_config {
    enable_private_endpoint = true
    enable_private_nodes    = true
    master_global_access_config {
      enabled = true
    }
  }
  ip_allocation_policy {}
}
