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


module "setup" {
  source             = "./setup"
  project_id         = var.project_id
  service_account_id = var.service_account_id
}

module "network" {
  depends_on      = [module.setup]
  source          = "./network"
  project_id      = var.project_id
  region          = var.network_region
  network_name    = var.network_name
  subnetwork_name = var.subnetwork_name
}

module "buckets" {
  depends_on    = [module.setup]
  source        = "./buckets"
  project_id    = var.project_id
  #  terraform_bucket_name     = var.bucket_terraform_state_name
  #  terraform_storage_class   = var.bucket_terraform_state_storage_class
  #  terraform_bucket_location = var.bucket_terraform_state_location
  name          = var.bucket_examples_name
  storage_class = var.bucket_examples_storage_class
  location      = var.bucket_examples_location
}

module "artifact_registry" {
  depends_on = [module.setup, module.buckets]
  source     = "./artifact_registry"
  project_id = var.project_id
  id         = var.repository_id
  location   = var.repository_location
}

module "memorystore" {
  depends_on     = [module.setup, module.network]
  source         = "./memorystore"
  project_id     = var.project_id
  redis_version  = var.redis_version
  region         = var.redis_region
  name           = var.redis_name
  tier           = var.redis_tier
  replica_count  = var.redis_replica_count
  memory_size_gb = var.redis_memory_size_gb
  replicas_mode  = var.read_replicas_mode
  network        = module.network.playground_network_id
  subnetwork     = module.network.playground_subnetwork_id
}

module "gke" {
  depends_on            = [module.setup, module.artifact_registry, module.memorystore, module.network]
  source                = "./gke"
  project_id            = var.project_id
  service_account_email = module.setup.service_account_email
  machine_type      = var.gke_machine_type
  node_count        = var.gke_node_count
  name              = var.gke_name
  location          = var.gke_location
  subnetwork        = module.network.playground_subnetwork_id
  network           = module.network.playground_network_id
}
