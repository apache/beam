resource "kubernetes_config_map" "environment" {
  metadata {
    name      = "looker-environment"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  data = {
    LOOKER_REDIS_CACHE_DISCOVERY       = "redis://redis-master.looker.svc.cluster.local:${data.kubernetes_service.redis.spec[0].port[0].port}"
    LOOKER_REDIS_OPERATIONAL_DISCOVERY = "redis://redis-master.looker.svc.cluster.local:${data.kubernetes_service.redis.spec[0].port[0].port}"
    "LOOKERARGS"                       = join(" ", [
      "--no-ssl",
      "-d ${local.db_credentials_config_path}",
      "--clustered",
      "-H $POD_IP",
      "--shared-storage-dir ${local.lookerfiles_path}",
    ])
  }
}
