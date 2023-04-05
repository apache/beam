data "google_secret_manager_secret" "looker_license" {
  secret_id = var.license_secret_name
}

data "google_secret_manager_secret_version" "looker_license" {
  secret  = data.google_secret_manager_secret.looker_license.name
  version = "latest"
}

data "http" "looker_jar_metadata" {
  url             = var.looker_jar_endpoint
  method          = "POST"
  request_headers = {
    Content-Type = "application/json"
  }

  request_body = jsonencode({
    lic      = data.google_secret_manager_secret_version.looker_license.secret_data
    email    = var.license_email
    latest   = "specific"
    specific = "looker-${var.looker_version}-latest.jar"
  })
}

resource "null_resource" "temporary_directory" {
  provisioner "local-exec" {
    command = "mkdir -p ${local.temporary.looker}"
  }
}

resource "null_resource" "looker_jar" {
  depends_on = [null_resource.temporary_directory]
  triggers   = {
    sha256 = jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.sha256]
  }
  provisioner "local-exec" {
    command = "wget -O \"${local.temporary.looker}/${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.version_text])}\" \"${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.url])}\""
  }
}

resource "null_resource" "looker_dependencies_jar" {
  depends_on = [null_resource.temporary_directory]
  triggers   = {
    depSha256 = jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.depSha256]
  }
  provisioner "local-exec" {
    command = "wget -O \"${local.temporary.looker}/${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.depDisplayFile])}\" \"${trimspace(jsondecode(data.http.looker_jar_metadata.response_body)[local.looker_jar_metadata_keys.depUrl])}\""
  }
}

resource "null_resource" "prometheus_jar" {
  depends_on = [null_resource.temporary_directory]
  triggers   = {
    sha256 = fileexists(local.prometheus.target_path) ? filesha256(local.prometheus.target_path) : ""
  }
  provisioner "local-exec" {
    command = "wget -O \"${local.prometheus.target_path}\" ${local.prometheus.url}"
  }
}