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


data "terraform_remote_state" "remote_state_vpc" {
  backend = "gcs"
  config = {
    bucket = var.terraform_state_bucket_name
  }
}

data "google_compute_network" "default" {
  project = var.project_id
  name = var.network
}

# Redis for storing state of Playground application.
# In this cache Playground instances stores pipeline's statuses, outputs and pipeline's graph
resource "google_redis_instance" "cache" {
  // TODO: remove when replica_count, etc is generally available
  provider       = google-beta
  project        = var.project_id
  region         = var.redis_region
  name           = var.redis_name
  tier           = var.redis_tier
  memory_size_gb = var.redis_memory_size_gb
  replica_count  = var.redis_replica_count
  redis_version      = var.redis_version
  display_name       = var.display_name
  read_replicas_mode = var.read_replicas_mode
  authorized_network = data.google_compute_network.default.id
}
