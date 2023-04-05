locals {
  substitutions = [
    "_LOOKER_JARS_GLOB=${var.looker_jars_glob}",
    "_REGISTRY_PATH=${var.artifact_registry_url}",
    "_IMAGE_NAME=${var.resource_name_prefix}",
  ]
}
resource "null_resource" "build_image" {
  provisioner "local-exec" {
    command = "gcloud builds submit . --substitutions=${join(",", local.substitutions)}"
  }
}
