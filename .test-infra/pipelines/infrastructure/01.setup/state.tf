terraform {
  backend "gcs" {
    bucket = "b507e468-52e9-4e72-83e5-ecbf563eda12"
    prefix = "terraform/state/github.com/apache/beam/.test-infra/pipelines/infrastructure/01.setup"
  }
}
