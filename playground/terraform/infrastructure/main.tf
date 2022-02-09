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

provider "google" {
  region = "us-central"
}
provider "google-beta" {
  region = "us-central"
}


module "vpc" {
  source         = "./vpc"
  project_id     = var.project_id
  create_subnets = var.create_subnets
  mtu            = var.mtu
  vpc_name       = var.vpc_name
}

module "buckets" {
  source                    = "./buckets"
  project_id                = var.project_id
  terraform_bucket_name     = var.terraform_bucket_name
  terraform_storage_class   = var.examples_storage_class
  terraform_bucket_location = var.terraform_bucket_location
  examples_bucket_name      = var.examples_bucket_name
  examples_storage_class    = var.examples_storage_class
  examples_bucket_location  = var.examples_bucket_location
}

module "artifact_registry" {
  source              = "./artifact_registry"
  project_id          = var.project_id
  repository_id       = var.repository_id
  repository_location = var.repository_location
  depends_on          = [module.buckets]
}

module "memorystore" {
  source                      = "./memorystore"
  project_id                  = var.project_id
  terraform_state_bucket_name = var.terraform_bucket_name
  redis_version               = var.redis_version
  redis_region                = var.redis_region
  redis_name                  = var.redis_name
  redis_tier                  = var.redis_tier
  redis_replica_count         = var.redis_replica_count
  redis_memory_size_gb        = var.redis_memory_size_gb
  depends_on                  = [module.artifact_registry]
}

module "gke" {
  source           = "./gke"
  project_id       = var.project_id
  service_account  = var.service_account
  gke_machine_type = var.gke_machine_type
  gke_node_count   = var.gke_node_count
  gke_name         = var.gke_name
  gke_location     = var.gke_location
  depends_on       = [module.artifact_registry, module.memorystore]
}


