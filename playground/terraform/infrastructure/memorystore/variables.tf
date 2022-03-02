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

variable "redis_version" {
  description = "The GCP Project ID where Playground Applications will be created"
  default     = "REDIS_6_X"
}

variable "terraform_state_bucket_name" {
  description = "Bucket name for terraform state"
  default     = "beam_playground_terraform"
}

variable "redis_region" {
  description = "Region of Redis"
  default     = "us-central1"
}

variable "redis_name" {
  description = "Name of Redis"
  default     = "playground-backend-cache"
}

variable "redis_tier" {
  description = "Tier of Redis"
  default     = "STANDARD_HA"
}

variable "redis_replica_count" {
  type        = number
  description = "Redis's replica count"
  default     = 1
}

variable "redis_memory_size_gb" {
  type        = number
  description = "Size of Redis memory"
  default     = 5
}

variable "display_name" {
  default     = "Playground Cache"
  description = "Display name for Redis service"
}

variable "read_replicas_mode" {
  description = "Read replica mode. Can only be specified when trying to create the instance."
  default     = "READ_REPLICAS_ENABLED"
}

variable "network" {
  description = "GCP network within which resources are provisioned"
}

variable "subnetwork" {
  description = "GCP subnetwork within which resources are provisioned"
}
