resource "random_password" "gcm" {
  length = 32
}
resource "kubernetes_secret" "gcm" {
  metadata {
    name      = "gcm-key"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  data = {
    LKR_MASTER_KEY_ENV = base64encode(random_password.gcm.result)
  }
}
