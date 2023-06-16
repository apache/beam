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

resource "kubernetes_config_map" "quota" {
  metadata {
    name      = var.quota_service.name
    namespace = data.kubernetes_namespace.default.metadata[0].name
  }
  data = {
    "NAMESPACE" : data.kubernetes_namespace.default.metadata[0].name
    "PORT" : tostring(var.quota_service.port)
    "CACHE_HOST" : var.cache_host
    "LOG_LEVEL" : var.log_level
    "REFRESHER_IMAGE" : "${var.image_repository}/${var.refresher_service_image}"
  }
}

resource "kubernetes_deployment" "quota" {
  metadata {
    name      = var.quota_service.name
    namespace = data.kubernetes_namespace.default.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        app = var.quota_service.name
      }
    }
    template {
      metadata {
        labels = {
          app = var.quota_service.name
        }
      }
      spec {
        service_account_name = var.service_account_name
        container {
          name              = var.quota_service.name
          image             = "${var.image_repository}/${var.quota_service.image}"
          image_pull_policy = "IfNotPresent"
          port {
            container_port = var.quota_service.port
          }
          env_from {
            config_map_ref {
              name = kubernetes_config_map.quota.metadata[0].name
            }
          }
        }
      }
    }
  }
}
