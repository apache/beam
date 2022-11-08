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

output "connect_githubrepo_to_cloudbuild" {
  value = <<EOF

Navigate to https://console.cloud.google.com/cloud-build/triggers/connect?project=${var.project_id}
to connect Cloud Build to your GitHub repository.
(Note: skip where it asks you to create a trigger.)
EOF
}

output "next_step_custom_message_hack" {
  value = <<EOF

As a one-time setup,
navigate to https://console.cloud.google.com/cloud-build/triggers?project=${var.project_id}
and click `RUN` for ${module.cloudbuild_trigger.cloudbuild_trigger_name}

(Note: You will have to manually start the trigger)

EOF
}

