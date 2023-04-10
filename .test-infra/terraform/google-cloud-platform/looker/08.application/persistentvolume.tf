resource "kubernetes_persistent_volume_claim" "default" {
  metadata {
    name      = local.looker_persistent_volume_claim_name
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = "10Gi"
      }
    }
  }
}