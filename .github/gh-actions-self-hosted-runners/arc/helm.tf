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
resource "helm_release" "cert-manager" {
  name       = "cert-manager"
  namespace  = "cert-manager"
  create_namespace = true
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  
  atomic = "true"
  timeout = 100

  set {
    name  = "installCRDs"
    value = "true"
  }
  depends_on = [ google_container_node_pool.main-actions-runner-pool ]
}

resource "helm_release" "arc" {
  name = "arc"
  namespace = "arc"
  create_namespace = "true"
  repository = "https://actions-runner-controller.github.io/actions-runner-controller"
  chart = "actions-runner-controller"

  atomic = "true"
  timeout = 120

  dynamic "set" {
    for_each = local.arc_values
    content {
      name = set.key
      value = set.value
    }
  }
  depends_on = [ helm_release.cert-manager ]
}
