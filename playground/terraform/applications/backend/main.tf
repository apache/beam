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



module "backend-go" {
  source                  = "./backend-go"
  project_id              = var.project_id
  cache_address           = var.cache_address
  docker_registry_address = var.docker_registry_address
  network_name            = var.network_name
  subnetwork_name         = var.subnetwork_name
  environment             = var.environment
  docker_image_name       = "${var.docker_image_name}-go"
  docker_image_tag        = var.docker_image_tag
  service_name            = "${var.base_service_name}-go"
  cache_type              = var.cache_type
  volume_size             = var.go_volume_size
}

module "backend-java" {
  source                  = "./backend-java"
  project_id              = var.project_id
  cache_address           = var.cache_address
  docker_registry_address = var.docker_registry_address
  network_name            = var.network_name
  subnetwork_name         = var.subnetwork_name
  environment             = var.environment
  docker_image_name       = "${var.docker_image_name}-java"
  docker_image_tag        = var.docker_image_tag
  service_name            = "${var.base_service_name}-java"
  cache_type              = var.cache_type
  volume_size             = var.java_volume_size
}

module "backend-python" {
  source                  = "./backend-python"
  project_id              = var.project_id
  cache_address           = var.cache_address
  docker_registry_address = var.docker_registry_address
  network_name            = var.network_name
  subnetwork_name         = var.subnetwork_name
  environment             = var.environment
  docker_image_name       = "${var.docker_image_name}-python"
  docker_image_tag        = var.docker_image_tag
  service_name            = "${var.base_service_name}-python"
  cache_type              = var.cache_type
  volume_size             = var.python_volume_size
}

module "backend-router" {
  source                  = "./backend-router"
  project_id              = var.project_id
  cache_address           = var.cache_address
  docker_registry_address = var.docker_registry_address
  network_name            = var.network_name
  subnetwork_name         = var.subnetwork_name
  environment             = var.environment
  docker_image_name       = "${var.docker_image_name}-router"
  docker_image_tag        = var.docker_image_tag
  service_name            = "${var.base_service_name}-router"
  cache_type              = var.cache_type
  volume_size             = var.router_volume_size
}

module "backend-scio" {
  source                  = "./backend-scio"
  project_id              = var.project_id
  cache_address           = var.cache_address
  docker_registry_address = var.docker_registry_address
  network_name            = var.network_name
  subnetwork_name         = var.subnetwork_name
  environment             = var.environment
  docker_image_name       = "${var.docker_image_name}-scio"
  docker_image_tag        = var.docker_image_tag
  service_name            = "${var.base_service_name}-scio"
  cache_type              = var.cache_type
  volume_size             = var.scio_volume_size
}



