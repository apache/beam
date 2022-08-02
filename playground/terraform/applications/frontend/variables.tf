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
  description = "Project ID"
}

variable "docker_registry_address" {
  description = "Docker registry address"
}

variable "docker_image_name" {
  description = "Docker Image Name To Be Deployed"
  default     = "beam_playground-frontend"
}

variable "docker_image_tag" {
  description = "Docker Image Tag To Be Deployed"
  default     = "latest"
}

variable "service_name" {
  default = "frontend"
}

variable "network_name" {
  description = "VPC Name"
  default     = "playground-vpc"
}

variable "subnetwork_name" {
  description = "Vpc Name"
  default     = "playground-vpc"
}

variable "environment" {
  description = "prod,dev"
  default     = "dev"
}
