#resource "kubernetes_deployment" "default" {
#  metadata {
#    name      = "looker"
#    namespace = data.kubernetes_namespace.looker.metadata[0].name
#    labels    = {
#      app = "looker"
#    }
#  }
#  spec {
#    selector {
#      match_labels = {
#        app = "looker"
#      }
#    }
#    template {
#      metadata {
#        labels = {
#          app = "looker"
#        }
#      }
#      spec {
#        container {
#          name  = "looker"
#          image = var.looker_image
#          env_from {
#            config_map_ref {
#              name = ""
#            }
#          }
#        }
#      }
#    }
#  }
#}
