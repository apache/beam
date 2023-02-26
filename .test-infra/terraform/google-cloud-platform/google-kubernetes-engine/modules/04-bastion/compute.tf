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

locals {
  tinyproxy_content = file("${path.module}/tinyproxy.conf")
}

data "google_compute_zones" "available" {
  region = var.region
}

// Provision bastion host for private cluster connectivity.
// See https://cloud.google.com/kubernetes-engine/docs/tutorials/private-cluster-bastion
resource "google_compute_instance" "bastion" {
  depends_on   = [google_project_service.compute]
  machine_type = var.bastion_compute_machine_type
  name         = "bastion-${random_string.postfix.result}"
  zone         = data.google_compute_zones.available.names[0]
  service_account {
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    email = var.kubernetes_node_service_account.email
  }
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }
  network_interface {
    network    = var.network.id
    subnetwork = var.subnetwork.id
  }

  metadata_startup_script = <<EOF
apt install tinyproxy
cat << TP > /etc/tinyproxy/tinyproxy.conf
${local.tinyproxy_content}
TP
service tinyproxy restart
EOF
}