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
resource "kubectl_manifest" "arc_deployment" {
  yaml_body = templatefile("config/arc_deployment.tpl", { repository = "${var.organization}/${var.repository}" , group = var.runner_group})
  override_namespace = "arc"
  depends_on = [ helm_release.arc ]
}
resource "kubectl_manifest" "arc_autoscaler" {
  yaml_body = templatefile("config/arc_autoscaler.tpl", { min_runners = var.min_main_replicas, max_runners = var.max_main_replicas , webhook_scaling = var.webhook_scaling})
  override_namespace = "arc"
  depends_on = [ helm_release.arc ]
}
resource "kubectl_manifest" "arc_webhook_certificate" {
  yaml_body = templatefile("config/arc_certificate.tpl", { ingress_domain = var.ingress_domain })
  override_namespace = "arc"
  depends_on = [ helm_release.arc ]
}
