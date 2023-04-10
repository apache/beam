data "kubernetes_namespace" "looker" {
  metadata {
    name = "looker"
  }
}

data "kubernetes_secret" "credentials" {
  metadata {
    name      = var.bitami_mysql_credentials_secret
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
}
