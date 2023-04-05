resource "helm_release" "mysql-operator" {
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "mysql"
  name       = "mysql"
  namespace  = data.kubernetes_namespace.looker.metadata[0].name
  set {
    name  = "auth.existingSecret"
    value = kubernetes_secret.root_credentials.metadata[0].name
  }
}
