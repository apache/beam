// See: https://github.com/bitnami/charts/tree/main/bitnami/mysql
resource "helm_release" "mysql_operator" {
  timeout    = 3600 // 1 hour
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "mysql"
  name       = "mysql"
  namespace  = data.kubernetes_namespace.looker.metadata[0].name
  set {
    name  = "auth.existingSecret"
    value = data.kubernetes_secret.credentials.metadata[0].name
  }

  set {
    name  = "auth.username"
    value = "looker"
  }

  set {
    name  = "auth.database"
    value = "looker"
  }

  // See: https://cloud.google.com/looker/docs/migrating-looker-backend-db-to-mysql
  set {
    name  = "primary.configuration"
    value = <<EOF
[mysqld]
default_authentication_plugin=mysql_native_password
skip-name-resolve
explicit_defaults_for_timestamp
basedir=/opt/bitnami/mysql
plugin_dir=/opt/bitnami/mysql/lib/plugin
port=3306
socket=/opt/bitnami/mysql/tmp/mysql.sock
datadir=/bitnami/mysql/data
tmpdir=/opt/bitnami/mysql/tmp
max_allowed_packet=1073741824
bind-address=*
pid-file=/opt/bitnami/mysql/tmp/mysqld.pid
log-error=/opt/bitnami/mysql/logs/mysqld.log
character-set-server=utf8mb4
collation-server=utf8mb4_general_ci
slow_query_log=0
slow_query_log_file=/opt/bitnami/mysql/logs/mysqld.log
long_query_time=10.0
internal_tmp_mem_storage_engine = MEMORY

[client]
port=3306
socket=/opt/bitnami/mysql/tmp/mysql.sock
default-character-set=utf8mb4
plugin_dir=/opt/bitnami/mysql/lib/plugin

[manager]
port=3306
socket=/opt/bitnami/mysql/tmp/mysql.sock
pid-file=/opt/bitnami/mysql/tmp/mysqld.pid

EOF
  }
}
