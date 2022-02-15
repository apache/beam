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
  source                   = "./infrastructure"
  project_id               = var.project_id
  environment              = var.environment
  region                   = var.region
  #GCS
  examples_bucket_name     = var.examples_bucket_name
  examples_bucket_location = var.examples_bucket_location
  examples_storage_class   = var.examples_storage_class
  #Artifact Registry
  repository_id            = var.repository_id
  repository_location      = var.repository_location
  #Redis
  redis_version            = var.redis_version
  redis_name               = var.redis_name
  redis_tier               = var.redis_tier
  redis_replica_count      = var.redis_replica_count
  redis_memory_size_gb     = var.redis_memory_size_gb
  #VPC
  vpc_name                 = var.vpc_name
  create_subnets           = var.create_subnets
  mtu                      = var.mtu
  #GKE
  gke_machine_type         = var.gke_machine_type
  gke_node_count           = var.gke_node_count
  gke_name                 = var.gke_name
  gke_location             = var.gke_location
  service_account          = var.service_account

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

  go_volume_size     = var.go_volume_size
  java_volume_size   = var.java_volume_size
  python_volume_size = var.python_volume_size
  router_volume_size = var.router_volume_size
  scio_volume_size   = var.scio_volume_size


}



