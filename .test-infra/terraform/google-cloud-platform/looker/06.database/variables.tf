variable "bitami_mysql_credentials_secret" {
  type        = string
  description = <<EOF
The name of the secret storing Bitami MySql credentials. See `auth.existingSecret`
in https://github.com/bitnami/charts/tree/main/bitnami/mysql
EOF
}
