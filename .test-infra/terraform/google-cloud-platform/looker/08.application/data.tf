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

data "kubernetes_secret" "gcm_key" {
  metadata {
    name      = local.gcm_key_secret_id
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
}

data "kubernetes_secret" "looker_database_credentials" {
  metadata {
    name      = local.looker_database_credentials_secret_id
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
}
