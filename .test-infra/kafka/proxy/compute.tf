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

locals {

  // builds kafka-proxy's bootstrap-server-mapping flag based on the provided bootstrap_endpoint_mapping variable.
  bootstrap_server_mapping = join(" ", [
    for k, v in var.bootstrap_endpoint_mapping :"--bootstrap-server-mapping=\"${k},0.0.0.0:${v}\""
  ])

  // builds the gcloud ssh-flag to output after provisioning the bastion host based on the provided bootstrap_endpoint_mapping variable.
  ssh_flag = join(" ", [
    for port in values(var.bootstrap_endpoint_mapping) :"--ssh-flag=\"-4 -L${port}:localhost:${port}\""
  ])
}

// Provision a firewall allowing for Identity Aware Proxy (IAP) SSH traffic.
resource "google_compute_firewall" "allow-ssh-ingress-from-iap" {
  name      = "${data.google_compute_network.default.name}-allow-ssh-ingress-from-iap"
  network   = data.google_compute_network.default.id
  direction = "INGRESS"
  allow {
    protocol = "TCP"
    ports    = [22]
  }
  source_ranges = [var.allows_iap_ingress_cidr_range]
}

// Generate a random string to use as a postfix for resource naming.
resource "random_string" "postfix" {
  length  = 4
  upper   = false
  lower   = true
  numeric = true
  special = false
}

// Provision a bastion host for use as a kafka proxy.
// On launch, installs and executes https://github.com/grepplabs/kafka-proxy.
resource "google_compute_instance" "bastion" {
  machine_type = var.machine_type
  name         = "kafka-proxy-${random_string.postfix.result}"
  tags         = ["kafka-proxy"]
  zone         = data.google_compute_zones.available.names[0]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = "10"
    }
  }
  // Configures private IP only network interface.
  network_interface {
    subnetwork = data.google_compute_subnetwork.default.id
  }
  service_account {
    email = data.google_service_account.default.email
    // Defer to IAM roles
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
  metadata_startup_script = <<EOF
$(gcloud info --format="value(basic.python_location)") -m pip install numpy
curl -Ls https://github.com/grepplabs/kafka-proxy/releases/download/${var.kafka_proxy_version}/kafka-proxy-${var.kafka_proxy_version}-linux-amd64.tar.gz | tar xz
./kafka-proxy server ${local.bootstrap_server_mapping}
EOF
}

// Outputs the gcloud command to tunnel kafka proxy traffic to the local machine.
output "gcloud_tunnel_command" {
  value = <<EOF
  gcloud compute ssh ${google_compute_instance.bastion.name} --tunnel-through-iap --project=${var.project} --zone=${google_compute_instance.bastion.zone} ${local.ssh_flag}
EOF
}
