resource "kubernetes_service" "default" {
  metadata {
    name      = "looker"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  spec {
    selector = {
      app = "looker"
    }
    type = "NodePort"
    port {
      name = "web"
      port = 9999
    }
    port {
      name = "api"
      port = 19999
    }
  }
}
