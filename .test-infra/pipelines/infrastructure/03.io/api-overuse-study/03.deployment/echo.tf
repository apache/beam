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

resource "kubernetes_config_map" "echo" {
  metadata {
    name      = var.echo_service.name
    namespace = data.kubernetes_namespace.default.metadata[0].name
  }
  data = {
    "PORT" : tostring(var.echo_service.port)
    "CACHE_HOST" : var.cache_host
    "LOG_LEVEL" : var.log_level
  }
}

resource "kubernetes_deployment" "echo" {
  metadata {
    name      = var.echo_service.name
    namespace = data.kubernetes_namespace.default.metadata[0].name
  }
  spec {
    selector {
      match_labels = {
        app = var.echo_service.name
      }
    }
    template {
      metadata {
        labels = {
          app = var.echo_service.name
        }
      }
      spec {
        container {
          name              = var.echo_service.name
          image             = "${var.image_repository}/${var.echo_service.image}"
          image_pull_policy = "IfNotPresent"
          port {
            container_port = var.echo_service.port
          }
          env_from {
            config_map_ref {
              name = kubernetes_config_map.echo.metadata[0].name
            }
          }
        }
      }
    }
  }
}