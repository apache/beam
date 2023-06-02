terraform {
  backend "gcs" {
    # bucket configured in *.tfbackend file
    prefix = "terraform/state/github.com/apache/beam/.test-infra/pipelines/infrastructure/01.setup"
  }
}
