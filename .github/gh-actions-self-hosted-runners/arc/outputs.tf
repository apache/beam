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

output "cluster_name" {
    value = google_container_cluster.actions-runner-gke.name
}
output "cluster_endpoint" {
    value = google_container_cluster.actions-runner-gke.endpoint
}
output "ingress_ip" {
    value = var.deploy_webhook != "false" ? data.google_compute_global_address.actions-runner-ip[0].address : "Not Configured"
}
output "get_kubeconfig_command" {
    value = "gcloud container clusters get-credentials ${google_container_cluster.actions-runner-gke.name} --region ${var.zone} --project ${var.project_id}"
}
  
