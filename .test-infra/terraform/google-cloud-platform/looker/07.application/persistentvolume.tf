resource "kubernetes_persistent_volume_claim" "default" {
  metadata {
    name      = "looker-pvc"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  spec {
    access_modes = ["ReadWriteMany"]
    resources {
      requests = {
        storage = "10Gi"
      }
    }
    storage_class_name = "standard-rwo"
  }
}