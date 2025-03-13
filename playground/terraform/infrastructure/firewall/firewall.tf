# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_compute_firewall" "playground-firewall-deny-egress" {
  name    = "${var.network_name}-deny-egress"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1001
  deny {
    protocol      = "all"
  }
  destination_ranges = ["0.0.0.0/0"]
  target_tags = ["beam-playground"]
}

resource "google_compute_firewall" "playground-firewall-allow-controlplane" {
  name    = "${var.network_name}-allow-controlplane"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol      = "all"
  }
  destination_ranges = [var.gke_controlplane_cidr]
  target_tags = ["beam-playground"]
}

resource "google_compute_firewall" "playground-firewall-allow-dns" {
  name    = "${var.network_name}-allow-dns"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "tcp"
    ports    = ["53"]

  }
  allow {
    protocol = "udp"
    ports = ["53"]
  }
  destination_ranges = ["0.0.0.0/0"]
  target_tags = ["beam-playground"]
}

# Allows for private google access as per:
# https://cloud.google.com/vpc/docs/configure-private-google-access#config
resource "google_compute_firewall" "playground-firewall-allow-privateapi" {
  name    = "${var.network_name}-allow-privateapi"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "all"
  }

  destination_ranges = [local.private_api_range]
  target_tags = ["beam-playground"]
}


resource "google_compute_firewall" "playground-firewall-allow-redis" {
  name    = "${var.network_name}-allow-redis"
  network = var.network_name
  direction     = "EGRESS"
  priority      = 1000
  allow {
    protocol = "all"
  }

  destination_ranges = [var.redis_ip]
  target_tags = ["beam-playground"]
}