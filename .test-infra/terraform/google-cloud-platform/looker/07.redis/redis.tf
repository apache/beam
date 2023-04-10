resource "helm_release" "redis" {
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "redis"
  name       = "redis"
  namespace  = data.kubernetes_namespace.looker.metadata[0].name
  set {
    name  = "auth.enabled"
    value = false
  }
  set {
    name  = "auth.sentinel"
    value = false
  }
}
