// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Provision the Kubernetes cluster.
resource "google_container_cluster" "default" {
  depends_on          = [google_project_service.required]
  deletion_protection = false
  name                = local.resource_name
  location            = var.region
  enable_autopilot    = true
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
  }
  network    = data.google_compute_network.default.id
  subnetwork = data.google_compute_network.default.id
  node_config {
    service_account = google_service_account.node_pool.email
  }
}
