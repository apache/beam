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

locals {
  resource_name = "${var.resource_name_prefix}-${random_string.postfix.result}"
}

resource "google_project_service" "required" {
  for_each = toset([
    "artifactregistry",
    "cloudresourcemanager",
    "container",
    "iam",
  ])
  service            = "${each.key}.googleapis.com"
  disable_on_destroy = false
}

resource "random_string" "postfix" {
  length  = 6
  special = false
  upper   = false
}

// Query the VPC network.
data "google_compute_network" "default" {
  name = var.network
}

// Query valid subnetwork configuration.
data "google_compute_subnetwork" "default" {
  name   = var.subnetwork
  region = var.region
  lifecycle {
    postcondition {
      condition     = self.private_ip_google_access
      error_message = "The subnetwork: regions/${var.region}/subnetworks/${var.subnetwork} in projects/${var.project}/networks/${var.network} does not have private google access enabled"
    }
  }
}

// Query valid existence of the router.
data "google_compute_router" "default" {
  name    = var.router
  region  = var.region
  network = data.google_compute_network.default.id
}
