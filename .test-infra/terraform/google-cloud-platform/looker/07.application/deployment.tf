resource "kubernetes_deployment" "default" {
  timeouts {
    create = "60m"
  }
  metadata {
    name      = "looker"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
    labels    = {
      app = "looker"
    }
  }
  spec {
    selector {
      match_labels = {
        app = "looker"
      }
    }
    template {
      metadata {
        labels = {
          app = "looker"
        }
      }
      spec {
        volume {
          name = "looker-pv"
          persistent_volume_claim {
            claim_name = local.looker_persistent_volume_claim_name
          }
        }
        volume {
          name = "looker-db-config"
          secret {
            secret_name = data.kubernetes_secret.looker_db_credentials.metadata[0].name
          }
        }
        init_container {
          name              = "looker-data-ownership-grant"
          image             = "gcr.io/google-containers/busybox:latest"
          image_pull_policy = "IfNotPresent"
          command           = [
            "sh", "-c", "chown -R 9999:9999 ${local.lookerfiles_path}"
          ]
          volume_mount {
            mount_path = local.lookerfiles_path
            name       = "looker-pv"
          }
          resources {
            limits = {
              cpu    = "2000m"
              memory = "2Gi"
            }
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }
        }
        container {
          name              = "looker"
          image             = var.looker_image
          image_pull_policy = "IfNotPresent"
          resources {
            limits = {
              cpu    = "2000m"
              memory = "2Gi"
            }
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }
          volume_mount {
            mount_path = local.lookerfiles_path
            name       = "looker-pv"
          }
          volume_mount {
            mount_path = local.var_dir
            name       = "looker-db-config"
            read_only  = true
          }
          env_from {
            secret_ref {
              name = kubernetes_secret.gcm.metadata[0].name
            }
          }
          env_from {
            config_map_ref {
              name = kubernetes_config_map.environment.metadata[0].name
            }
          }
          env {
            name = "POD_IP"
            value_from {
              field_ref {
                field_path = "status.podIP"
              }
            }
          }
          port {
            container_port = 9999
            name           = "web"
          }
          port {
            container_port = 19999
            name           = "api"
          }
          readiness_probe {
            http_get {
              path = "/alive"
              port = "9999"
            }
            initial_delay_seconds = 90
            period_seconds        = 5
            success_threshold     = 2
          }
        }
      }
    }
  }
}
