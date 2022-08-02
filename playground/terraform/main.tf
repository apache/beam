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

module "infrastructure" {
  source                        = "./infrastructure"
  project_id                    = var.project_id
  environment                   = var.environment
  region                        = var.region
  #GCS
  bucket_examples_name          = var.bucket_examples_name
  bucket_examples_location      = var.bucket_examples_location
  bucket_examples_storage_class = var.bucket_examples_storage_class
  #Artifact Registry
  repository_id                 = var.repository_id
  repository_location           = var.repository_location
  #Redis
  redis_version                 = var.redis_version
  redis_name                    = var.redis_name
  redis_tier                    = var.redis_tier
  redis_replica_count           = var.redis_replica_count
  redis_memory_size_gb          = var.redis_memory_size_gb
  #NETWORK
  network_name                  = var.network_name
  #GKE
  gke_machine_type              = var.gke_machine_type
  gke_node_count                = var.gke_node_count
  gke_name                      = var.gke_name
  gke_location                  = var.gke_location
  service_account               = var.service_account
}

module "applications" {
  source                = "./applications"
  project_id            = var.project_id
  environment           = var.environment
  docker_image_name     = var.docker_image_name
  docker_image_tag      = var.docker_image_tag
  backend_service_name  = var.backend_service_name
  frontend_service_name = var.frontend_service_name
  cache_type            = var.cache_type

  go_volume_size      = var.go_volume_size
  go_cpu              = var.go_cpu
  go_memory           = var.go_memory
  go_max_instance     = var.go_max_instance
  go_min_instance     = var.go_min_instance
  java_volume_size    = var.java_volume_size
  java_cpu            = var.java_cpu
  java_memory         = var.java_memory
  java_max_instance   = var.java_max_instance
  java_min_instance   = var.java_min_instance
  python_volume_size  = var.python_volume_size
  python_cpu          = var.python_cpu
  python_memory       = var.python_memory
  python_max_instance = var.python_max_instance
  python_min_instance = var.python_min_instance
  router_volume_size  = var.router_volume_size
  router_cpu          = var.router_cpu
  router_memory       = var.router_memory
  router_max_instance = var.router_max_instance
  router_min_instance = var.router_min_instance
  scio_volume_size    = var.scio_volume_size
  scio_cpu            = var.scio_cpu
  scio_memory         = var.scio_memory
  scio_max_instance   = var.scio_max_instance
  scio_min_instance   = var.scio_min_instance

  location               = var.application_location
  create_default_service = var.create_default_service
  state_bucket           = var.state_bucket
  state_prefix           = var.state_prefix
}



