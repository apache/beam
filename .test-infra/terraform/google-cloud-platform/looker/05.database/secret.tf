resource "random_password" "mysql_root_password" {
  length           = 8
  min_lower        = 1
  min_upper        = 1
  min_special      = 1
  min_numeric      = 1
  override_special = "*"
}

resource "random_password" "mysql_replication_password" {
  length           = 8
  min_lower        = 1
  min_upper        = 1
  min_special      = 1
  min_numeric      = 1
  override_special = "*"
}

resource "random_password" "mysql_password" {
  length           = 8
  min_lower        = 1
  min_upper        = 1
  min_special      = 1
  min_numeric      = 1
  override_special = "*"
}

resource "kubernetes_secret" "root_credentials" {
  metadata {
    name      = "mysql-credentials"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  data = {
    mysql-root-password        = random_password.mysql_root_password.result
    mysql-replication-password = random_password.mysql_replication_password.result
    mysql-password             = random_password.mysql_password.result
  }
}
