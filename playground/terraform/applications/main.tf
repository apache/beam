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

data "terraform_remote_state" "playground-state" {
  backend = "gcs"
  config  = {
    bucket = var.terraform_state_bucket
    prefix = "terraform/state"
  }
}

module "backend-go" {
  source                  = "./backend-go"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.go_docker_image_name
  docker_image_tag        = var.go_docker_image_tag
  service_name            = "${var.backend_service_name}-go"
  cache_type              = var.go_cache_type
  volume_size             = var.go_volume_size
}

module "backend-java" {
  source                  = "./backend-java"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.java_docker_image_name
  docker_image_tag        = var.java_docker_image_tag
  service_name            = "${var.backend_service_name}-java"
  cache_type              = var.java_cache_type
  volume_size             = var.java_volume_size
}

module "backend-python" {
  source                  = "./backend-python"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.python_docker_image_name
  docker_image_tag        = var.python_docker_image_tag
  service_name            = "${var.backend_service_name}-python"
  cache_type              = var.python_cache_type
  volume_size             = var.python_volume_size
}

module "backend-router" {
  source                  = "./backend-router"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.router_docker_image_name
  docker_image_tag        = var.router_docker_image_tag
  service_name            = "${var.backend_service_name}-router"
  cache_type              = var.router_cache_type
  volume_size             = var.router_volume_size
}

module "backend-scio" {
  source                  = "./backend-scio"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.scio_docker_image_name
  docker_image_tag        = var.scio_docker_image_tag
  service_name            = "${var.backend_service_name}-scio"
  cache_type              = var.scio_cache_type
  volume_size             = var.scio_volume_size
}

module "frontend" {
  source                  = "./frontend"
  project_id              = var.project_id
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.playground_registry_id
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_vpc_name
  environment             = var.environment
  docker_image_name       = var.frontend_docker_image_name
  docker_image_tag        = var.frontend_docker_image_tag
  service_name            = var.frontend_service_name
}
