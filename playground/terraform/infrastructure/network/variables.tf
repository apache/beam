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

variable "region" {
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "network_name" {
  description = "Name of VPC to be created"
  default     = "playground-network"
}

variable "subnetwork_name" {
  description = "Name of VPC to be created"
  default     = "playground-subnetwork"
}

variable "subnetwork_cidr_range" {
  description = "The address range for this subnet, in CIDR notation. Use a standard private VPC network address range: for example, 10.0.0.0/9."
  default     = "10.128.0.0/20"
}
