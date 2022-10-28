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

resource "google_dns_managed_zone" "playground" {
  name        = "playground"
  dns_name    = var.dnsname
  description = "Playground DNS Zone"
}

resource "google_dns_record_set" "frontplayground" {
  managed_zone = google_dns_managed_zone.playground.name
  name    = "${google_dns_managed_zone.playground.dns_name}"
  type    = "A"
  rrdatas = ["data.terraform_remote_state.playground-state.outputs.playground_static_ip_address"]
  ttl     = 300
}

resource "google_dns_record_set" "playground" {
  managed_zone = google_dns_managed_zone.playground.name
  for_each = toset(var.names)
  name    = "each.value.${google_dns_managed_zone.playground.dns_name}"
  type    = "A"
  rrdatas = ["data.terraform_remote_state.playground-state.outputs.playground_static_ip_address"]
  ttl     = 300
}

resource "random_id" "rnd" {
  byte_length = 4
}
