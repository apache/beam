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

resource "google_dns_managed_zone" "private-zone-apis" {

  for_each = var.private_zones

  project     = var.project_id 

  name        = "${var.network_name}-${replace(each.key,".","-")}"
  dns_name    = format("%s%s",each.key,".")
  description = "Private ${each.key} Zone"
  
  visibility = "private"

  private_visibility_config {
    networks {
      network_url = var.network_id
    }
  }
}

resource "google_dns_record_set" "a-private-zone" {

  for_each = google_dns_managed_zone.private-zone-apis

  name = each.value.dns_name
  type = "A"
  ttl  = 300

  managed_zone = each.value.name

  rrdatas = local.private_api_ips
}

resource "google_dns_record_set" "cname-private-zone" {

  for_each = google_dns_managed_zone.private-zone-apis

  name         = "*.${each.value.dns_name}"
  managed_zone = each.value.name
  type         = "CNAME"
  ttl          = 300
  rrdatas      = [each.value.dns_name]
} 


