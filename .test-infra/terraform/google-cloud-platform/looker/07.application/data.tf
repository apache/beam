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

data "kubernetes_secret" "looker_db_credentials" {
  metadata {
    name      = "looker-db-credentials"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
}