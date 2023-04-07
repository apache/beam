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

resource "random_password" "looker_db_password" {
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
    mysql-password             = random_password.looker_db_password.result
  }
}

resource "kubernetes_secret" "looker_db_credentials" {
  metadata {
    name      = "looker-db-credentials"
    namespace = data.kubernetes_namespace.looker.metadata[0].name
  }
  data = {
    "looker-db.yml" = <<EOF
dialect: mysql
host: mysql.looker.svc.cluster.local
username: looker
password: ${random_password.looker_db_password.result}
database: looker
port: 3306
EOF
  }
}