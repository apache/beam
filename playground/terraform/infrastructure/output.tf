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

output "playground_registry_id" {
  value = module.artifact_registry.registry_id
}

output "playground_registry_name" {
  value = module.artifact_registry.registry_name
}

output "playground_registry_location" {
  value = module.artifact_registry.registry_location
}

output "playground_network_id" {
  value = module.network.playground_network_id
}

output "playground_subnetwork_id" {
  value = module.network.playground_subnetwork_id
}

output "playground_network_name" {
  value = module.network.playground_network_name
}

output "playground_subnetwork_name" {
  value = module.network.playground_subnetwork_name
}

output "playground_redis_ip" {
  value = module.memorystore.redis_ip
}

output "docker-repository-root" {
  value = "${module.artifact_registry.registry_location}${var.repository_domain}/${var.project_id}/${module.artifact_registry.registry_name}"
}

output "playground_static_ip_address" {
 value = module.ip_address.playground_static_ip_address
}

output "playground_gke_project" {
 value = module.gke.playground_gke_project
}

output "playground_static_ip_address_name" {
 value = module.ip_address.playground_static_ip_address_name
}

output "playground_function_cleanup_url" {
  value = module.cloudfunctions.playground_function_cleanup_url
}

output "playground_function_put_url" {
  value = module.cloudfunctions.playground_function_put_url
}

output "playground_function_view_url" {
  value = module.cloudfunctions.playground_function_view_url
}
