data "kubernetes_namespace" "looker" {
  metadata {
    name = "looker"
  }
}
