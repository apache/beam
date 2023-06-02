terraform {
  backend "gcs" {
    prefix = "terraform/state/github.com/apache/beam/.test-infra/pipelines/infrastructure/02.network"
  }
}
