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

data "google_compute_zones" "available" {
  depends_on = [google_project_service.compute]
  region     = var.region
}

// Query the Virtual Private Cloud (VPC) network.
data "google_compute_network" "default" {
  depends_on = [google_project_service.compute]
  name       = var.network.name
}

// Query the Virtual Private Cloud (VPC) subnetwork.
data "google_compute_subnetwork" "default" {
  depends_on = [google_project_service.compute]
  name       = var.subnetwork.name
  region     = var.region
}

// Query the Virtual Private Cloud (VPC) router NAT.
// The bastion host requires this.
data "google_compute_router" "default" {
  depends_on = [google_project_service.compute]
  name       = var.router.name
  network    = data.google_compute_network.default.name
  region     = data.google_compute_subnetwork.default.region
}
