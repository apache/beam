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
    bucket = var.state_bucket
    prefix = var.state_prefix
  }
}

module "default" {
  source                 = "./default"
  project_id             = var.project_id
  create_default_service = var.create_default_service
}

module "backend" {
  depends_on              = [module.default]
  source                  = "./backend"
  project_id              = var.project_id
  cache_address           = data.terraform_remote_state.playground-state.outputs.playground_redis_ip
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.docker-repository-root
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_network_name
  subnetwork_name         = data.terraform_remote_state.playground-state.outputs.playground_subnetwork_name
  environment             = var.environment
  docker_image_tag        = var.docker_image_tag == "" ? var.environment : var.docker_image_tag
  docker_image_name       = "${var.docker_image_name}-backend"
  cache_type              = var.cache_type

  go_volume_size     = var.go_volume_size
  java_volume_size   = var.java_volume_size
  python_volume_size = var.python_volume_size
  router_volume_size = var.router_volume_size
  scio_volume_size   = var.scio_volume_size
}

module "frontend" {
  depends_on              = [module.default]
  source                  = "./frontend"
  project_id              = var.project_id
  docker_registry_address = data.terraform_remote_state.playground-state.outputs.docker-repository-root
  network_name            = data.terraform_remote_state.playground-state.outputs.playground_network_name
  subnetwork_name         = data.terraform_remote_state.playground-state.outputs.playground_subnetwork_name
  environment             = var.environment
  docker_image_tag        = var.docker_image_tag == "" ? var.environment : var.docker_image_tag
  docker_image_name       = "${var.docker_image_name}-frontend"
  service_name            = var.frontend_service_name
}

