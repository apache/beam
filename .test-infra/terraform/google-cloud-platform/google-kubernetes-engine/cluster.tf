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

resource "random_string" "postfix" {
  length  = 6
  upper   = false
  special = false
}

resource "google_container_cluster" "default" {
  depends_on          = [google_project_service.required, google_project_iam_member.assign_gke_iam]
  deletion_protection = false
  name                = coalesce(var.cluster_name_override,"${var.cluster_name_prefix}-${random_string.postfix.result}")
  location            = var.region
  enable_autopilot    = true
  network             = data.google_compute_network.default.id
  subnetwork          = data.google_compute_subnetwork.default.id
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
  }

  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = data.google_service_account.default.email
      oauth_scopes    = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]

    }
  }
}