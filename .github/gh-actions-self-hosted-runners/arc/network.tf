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

resource "google_compute_network" "actions-runner-network" {
  count = var.existing_vpc_name == "" ? 1 : 0
  project = var.project_id
  name    = "${var.environment}-actions-runner-network"
  auto_create_subnetworks = false
}
data "google_compute_network" "actions-runner-network" {
  name = var.existing_vpc_name == "" ? google_compute_network.actions-runner-network[0].name : var.existing_vpc_name
  project = var.project_id
}


resource "google_compute_subnetwork" "actions-runner-subnetwork" {
  ip_cidr_range            = var.subnetwork_cidr_range
  name                     = "${var.environment}-actions-runner-subnetwork"
  network                  = data.google_compute_network.actions-runner-network.id
  region                   = var.region
  project                  = var.project_id
}
