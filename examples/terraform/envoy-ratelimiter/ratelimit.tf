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

# Wait for cluster propagation
# GKE Public Endpoint takes ~1-2 minutes to become globally routable after creation.
# This delay prevents "network is unreachable" errors during initial resource deployment.
resource "time_sleep" "wait_for_cluster" {
  create_duration = "60s"

  depends_on = [google_container_cluster.primary]
}

# Namespace
resource "kubernetes_namespace" "ratelimit_namespace" {
  metadata {
    name = var.namespace
  }

  depends_on = [time_sleep.wait_for_cluster]
}


# ConfigMap
resource "kubernetes_config_map" "ratelimit_config" {
  metadata {
    name      = "ratelimit-config"
    namespace = var.namespace
  }

  data = {
    "config.yaml" = var.ratelimit_config_yaml
  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Redis Deployment
resource "kubernetes_deployment" "redis" {
  metadata {
    name      = "redis"
    namespace = var.namespace
    labels = {
      app = "redis"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "redis"
      }
    }

    template {
      metadata {
        labels = {
          app = "redis"
        }
      }

      spec {
        container {
          image = var.redis_image
          name  = "redis"

          port {
            container_port = 6379
          }

          resources {
            requests = var.redis_resources.requests
            limits   = var.redis_resources.limits
          }
        }
      }
    }
  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Redis Service
resource "kubernetes_service" "redis" {
  metadata {
    name      = "redis"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "redis"
    }

    port {
      port        = 6379
      target_port = 6379
    }
  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Rate Limit Deployment
resource "kubernetes_deployment" "ratelimit" {
  metadata {
    name      = "ratelimit"
    namespace = var.namespace
    labels = {
      app = "ratelimit"
    }
  }

  spec {
    replicas = var.ratelimit_replicas

    selector {
      match_labels = {
        app = "ratelimit"
      }
    }

    template {
      metadata {
        labels = {
          app = "ratelimit"
        }
      }

      spec {
        container {
          name    = "ratelimit"
          image   = var.ratelimit_image
          command = ["/bin/ratelimit"]

          port {
            container_port = 8080
          }
          port {
            container_port = 8081
          }
          port {
            container_port = 6070
          }
          dynamic "port" {
            for_each = var.enable_metrics ? [1] : []
            content {
              name           = "metrics"
              container_port = 9090
            }
          }

          env {
            name  = "USE_PROMETHEUS"
            value = var.enable_metrics ? "true" : "false"
          }
          env {
            name  = "PROMETHEUS_ADDR"
            value = ":9090"
          }
          env {
            name  = "PROMETHEUS_PATH"
            value = "/metrics"
          }
          env {
            name  = "USE_STATSD"
            value = "false"
          }
          env {
            name  = "DISABLE_STATS"
            value = var.enable_metrics ? "false" : "true"
          }
          env {
            name  = "LOG_FORMAT"
            value = "json"
          }
          env {
            name  = "LOG_LEVEL"
            value = var.ratelimit_log_level
          }
          env {
            name  = "REDIS_SOCKET_TYPE"
            value = "tcp"
          }
          env {
            name  = "REDIS_URL"
            value = "redis:6379"
          }
          env {
            name  = "RUNTIME_ROOT"
            value = "/data"
          }
          env {
            name  = "RUNTIME_SUBDIRECTORY"
            value = "ratelimit"
          }
          env {
            name  = "RUNTIME_WATCH_ROOT"
            value = "false"
          }
          env {
            name  = "RUNTIME_IGNOREDOTFILES"
            value = "true"
          }
          env {
            name  = "CONFIG_TYPE"
            value = "FILE"
          }
          env {
            name  = "GRPC_MAX_CONNECTION_AGE"
            value = var.ratelimit_grpc_max_connection_age
          }
          env {
            name  = "GRPC_MAX_CONNECTION_AGE_GRACE"
            value = var.ratelimit_grpc_max_connection_age_grace
          }

          resources {
            requests = var.ratelimit_resources.requests
            limits   = var.ratelimit_resources.limits
          }

          volume_mount {
            name       = "config-volume"
            mount_path = "/data/ratelimit/config"
          }
        }



        volume {
          name = "config-volume"
          config_map {
            name = kubernetes_config_map.ratelimit_config.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [
    kubernetes_namespace.ratelimit_namespace,
    kubernetes_config_map.ratelimit_config,
    kubernetes_service.redis
  ]

  lifecycle {
    ignore_changes = [spec[0].replicas]
  }
}

resource "kubernetes_horizontal_pod_autoscaler_v2" "ratelimit" {
  metadata {
    name      = "ratelimit-hpa"
    namespace = var.namespace
  }

  spec {
    min_replicas = var.min_replicas
    max_replicas = var.max_replicas

    scale_target_ref {
      kind        = "Deployment"
      name        = kubernetes_deployment.ratelimit.metadata[0].name
      api_version = "apps/v1"
    }

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = var.hpa_cpu_target_percentage
        }
      }
    }

    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = var.hpa_memory_target_percentage
        }
      }
    }
  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Rate Limit Internal Service
resource "kubernetes_service" "ratelimit" {
  metadata {
    name      = "ratelimit"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "ratelimit"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
    port {
      name        = "grpc"
      port        = 8081
      target_port = 8081
    }
    port {
      name        = "debug"
      port        = 6070
      target_port = 6070
    }
    dynamic "port" {
      for_each = var.enable_metrics ? [1] : []
      content {
        name        = "metrics"
        port        = 9090
        target_port = 9090
      }
    }
  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Rate Limit External Service (LoadBalancer)
resource "kubernetes_service" "ratelimit_external" {
  metadata {
    name      = "ratelimit-external"
    namespace = var.namespace
    annotations = {
      "networking.gke.io/load-balancer-type" = "Internal"
    }
  }

  spec {
    type             = "LoadBalancer"
    load_balancer_ip = google_compute_address.ratelimit_ip.address

    selector = {
      app = "ratelimit"
    }

    port {
      name        = "grpc"
      port        = 8081
      target_port = 8081
    }
    port {
      name        = "debug"
      port        = 6070
      target_port = 6070
    }

  }

  depends_on = [kubernetes_namespace.ratelimit_namespace]
}

# Pod Monitoring
resource "kubernetes_manifest" "ratelimit_pod_monitoring" {
  manifest = {
    apiVersion = "monitoring.googleapis.com/v1"
    kind       = "PodMonitoring"
    metadata = {
      name      = "ratelimit-monitoring"
      namespace = var.namespace
    }
    spec = {
      selector = {
        matchLabels = {
          app = "ratelimit"
        }
      }
      endpoints = [
        {
          port = "metrics"
          path = "/metrics"
          interval = "15s"
        }
      ]
    }
  }
  depends_on = [
    kubernetes_deployment.ratelimit,
    time_sleep.wait_for_cluster
  ]
}
