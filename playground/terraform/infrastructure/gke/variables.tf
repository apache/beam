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

variable "project_id" {
  description = "The GCP Project ID where Playground Applications will be created"
}

variable "machine_type" {
  description = "Node pool machine types"
  default     = "e2-standard-4"
}

variable "node_count" {
  description = "Node pool size"
  default     = 1
}

variable "service_account_email" {
  description = "Service account email"
}

variable "name" {
  description = "Name of GKE cluster"
  default     = "playground-examples"
}

variable "location" {
  description = "Location of GKE cluster"
  default     = "us-central1-a"
}

variable "network" {
  description = "GCP network within which resources are provisioned"
}

variable "subnetwork" {
  description = "GCP subnetwork within which resources are provisioned"
}
