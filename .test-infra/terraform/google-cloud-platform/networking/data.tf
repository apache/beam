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

// Query the Virtual Private Cloud network
data "google_compute_network" "default" {
  name = var.network
}

// Query the Virtual Private Cloud subnetwork
// Additionally checks whether Private Google Access is true
data "google_compute_subnetwork" "default" {
  name   = var.subnetwork
  region = var.region
  lifecycle {
    postcondition {
      condition     = self.private_ip_google_access
      error_message = <<EOF
Private Google Access is false, want: true, subnetwork: ${self.self_link}"
Run the following command:
gcloud compute networks subnets update --project=${self.project} ${self.name} --region=${self.region} --enable-private-ip-google-access
EOF
    }
  }
}
