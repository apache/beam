data "kubernetes_namespace" "looker" {
  metadata {
    name = "looker"
  }
}

data "kubernetes_service" "redis" {
  metadata {
    name      = "redis-master"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
}
