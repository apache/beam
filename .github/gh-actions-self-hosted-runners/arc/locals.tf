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


locals {
        arc_values = {
            #fix dind issue
            "image.dindSidecarRepositoryAndTag" = "docker:24.0.7-dind-alpine3.18"
            "githubWebhookServer.enabled" = "${var.deploy_webhook}"
            "authSecret.create" = "true"
            "authSecret.github_app_id" = data.google_secret_manager_secret_version.github_app_id.secret_data
            "authSecret.github_app_installation_id" = data.google_secret_manager_secret_version.github_app_install_id.secret_data
            "authSecret.github_app_private_key" = data.google_secret_manager_secret_version.github_private_key.secret_data
            "githubWebhookServer.ingress.enabled" = "${var.deploy_webhook}"
            "githubWebhookServer.ingress.hosts[0].host" = var.ingress_domain
            "githubWebhookServer.ingress.hosts[0].paths[0].path" = "/"
            "githubWebhookServer.ingress.hosts[0].paths[0].pathType" = "ImplementationSpecific"
            "githubWebhookServer.service.type" = "NodePort"
            "githubWebhookServer.ingress.annotations.kubernetes\\.io/ingress\\.global-static-ip-name" = var.deploy_webhook != "false" ? data.google_compute_global_address.actions-runner-ip[0].name : "not-configured"
            "githubWebhookServer.ingress.annotations.networking\\.gke\\.io/managed-certificates" = "managed-cert"
            "githubWebhookServer.ingress.annotations.kubernetes\\.io/ingress\\.class" = "gce"
        }
}