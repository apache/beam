// Build the Dataflow template.
resource "google_cloudbuild_trigger" "default" {
  location = var.region
  source_to_build {
    ref       = "refs/head/${var.github_repository_branch}"
    repo_type = "GITHUB"
    uri       = "https://github.com/${var.github_repository_owner}/${var.github_repository_name}"
  }
  build {

    timeout = "1800s"

    step {
      id         = "${var.gradle_project}:shadowJar"
      name       = "gradle:jdk11"
      entrypoint = "gradle"
      args       = [
        "${var.gradle_project}:shadowJar",
      ]
    }

    step {
      id         = "${var.gradle_project}:template"
      name       = "gcr.io/google.com/cloudsdktool/cloud-sdk"
      entrypoint = "gcloud"
      args       = [
        "dataflow",
        "flex-template",
        "build",
        "gs://${data.google_storage_bucket.default.name}/templates/${var.gradle_project}.json",
        "--project=${var.project}",
        "--image-gcr-path=${var.region}-docker.pkg.dev/${var.project}/${data.google}/${var.template_image_prefix}",
        "--metadata-file=/workspace/.test-infra/pipelines/infrastructure/04.template/dataflow-to-bigquery/dataflow-template.json",
        "--sdk-language=JAVA",
        "--flex-template-base-image=JAVA11",
        "--jar=/workspace/.test-infra/pipelines/dataflow-to-bigquery/build/libs/beam-beam-test-infra-pipelines-dataflow-to-bigquery-latest.jar",
        "--env=FLEX_TEMPLATE_JAVA_MAIN_CLASS=org.apache.beam.testinfra.pipelines.ReadDataflowApiWriteBigQuery",
        "--network=${data.google_compute_network.default.name}",
        "--subnetwork=regions/${var.region}/subnetworks/${data.google_compute_subnetwork.default.name}",
        "--disable-public-ips",
        "--service-account-email=${data.google_service_account.dataflow_worker.email}"
      ]
    }
  }
}
