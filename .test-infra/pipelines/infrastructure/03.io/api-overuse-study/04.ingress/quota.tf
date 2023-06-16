/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Expose the quota service using a Kubernetes service.
// metadata.annotations and spec.type determine its exposure and vary
// between local i.e. minikube deployment and remote GKE deployment.
resource "kubernetes_service" "quota" {
  wait_for_load_balancer = false
  metadata {
    name        = var.quota_service.name
    namespace   = data.kubernetes_namespace.default.metadata[0].name
    annotations = var.annotations
  }
  spec {
    type     = var.service_type
    selector = {
      app = var.quota_service.name
    }
    port {
      name        = "tcp-port"
      port        = var.quota_service.port
      target_port = var.quota_service.target_port
      protocol    = "TCP"
    }
  }
}
