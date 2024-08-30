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
  yaml_body          = templatefile("config/arc_deployment.tpl", { organization = var.organization, group = var.runner_group, name = var.main_runner.name, image = var.main_runner.runner_image, labels = var.main_runner.labels, selector = var.main_runner.enable_selector, taint = var.main_runner.enable_taint, requests = var.main_runner.requests, limits = var.main_runner.limits})
  override_namespace = "arc"
  depends_on         = [helm_release.arc]
}
resource "kubectl_manifest" "arc_autoscaler" {
  yaml_body          = templatefile("config/arc_autoscaler.tpl", { name = var.main_runner.name, min_runners = var.main_runner.min_replicas, max_runners = var.main_runner.max_replicas, webhook_scaling = var.main_runner.webhook_scaling })
  override_namespace = "arc"
  depends_on         = [helm_release.arc]
}
resource "kubectl_manifest" "arc_webhook_certificate" {
  count = var.deploy_webhook != "false" ? 1 : 0
  yaml_body          = templatefile("config/arc_certificate.tpl", { ingress_domain = var.ingress_domain })
  override_namespace = "arc"
  depends_on         = [helm_release.arc]
}


resource "kubectl_manifest" "arc_deployment_additional" {
  for_each = {
    for index, runner_pool in var.additional_runner_pools : runner_pool.name => runner_pool
  }
  yaml_body          = templatefile("config/arc_deployment.tpl", { organization = var.organization, group = var.runner_group, name = each.value.name, image = each.value.runner_image, labels = each.value.labels, selector = each.value.enable_selector, taint = each.value.enable_taint , requests = each.value.requests, limits = each.value.limits})
  override_namespace = "arc"
  depends_on         = [helm_release.arc]
}
resource "kubectl_manifest" "arc_autoscaler_additional" {
  for_each = {
    for index, runner_pool in var.additional_runner_pools : runner_pool.name => runner_pool
  }
  yaml_body          = templatefile("config/arc_autoscaler.tpl", { name = each.value.name, min_runners = each.value.min_replicas, max_runners = each.value.max_replicas, webhook_scaling = each.value.webhook_scaling })
  override_namespace = "arc"
  depends_on         = [helm_release.arc]
}
