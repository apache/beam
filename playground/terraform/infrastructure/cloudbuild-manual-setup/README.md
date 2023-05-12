<!---
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Beam Playground Cloud Build Setup

This directory organizes Infrastructure-as-Code to provision dependent resources and set up Cloud Build for Beam Playground.
Cloud Build triggers created by terraform scripts from this directory automate steps described in [readme](https://github.com/apache/beam/blob/master/playground/terraform/README.md).

# Requirements:

1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
    - Security Admin
    - Service Account User
    - Secret Manager Admin
    
3. [Google Cloud Storage buckets](https://cloud.google.com/storage/docs/creating-buckets)for:
- Terraform state for Cloud Build triggers: \<triggers-state-bucket\>
- Cloud Build private logs: \<private-logs-bucket\>
- Cloud Build public logs: \<public-logs-bucket\>

4. DNS name for your Playground deployment instance

5. OS with installed software listed below:
- [Java](https://adoptopenjdk.net/)
- [Kubernetes Command Line Interface](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
- [HELM](https://helm.sh/docs/intro/install/)
- [Docker](https://docs.docker.com/engine/install/)
- [Terraform](https://www.terraform.io/downloads)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- [Kubectl authentication](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
- [GO](https://go.dev/doc/install)

6. [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) for CI trigger

# Prepare deployment configuration

1. Provide values for Terraform variables in `beam/playground/terraform/infrastructure/cloudbuild-manual-setup/common.tfvars` file:
```
playground_deploy_sa = "pg-cb-deploy"                                       # Service account for Initialize-Playground-environment trigger
playground_update_sa = "pg-cb-update"                                       # Service account for Deploy-Update-Playground-environment trigger
playground_ci_sa = "pg-cb-ci"                                               # SA name used for CI trigger
playground_cd_sa = "pg-cb-cd"                                               # SA name used for CD trigger
project_id = "PROJECT_ID"                                                   # GCP Project ID
webhook_trigger_secret_id = "playground-cicd-webhook"                       # Secret ID for webhook
data_for_cicd_webhook_secret = "secret_sting"                               # Secret used when creating the Github webhook
gh_pat_secret_id = "playground-github-pat-ci"                               # Secret ID with github PAT
data_for_github_pat_secret = "PAT"                                          # Actual Github PAT
trigger_source_repo = "https://github.com/beamplayground/deploy-workaround" # Trigger source repository. The repository must be connected to Cloud Build
trigger_source_branch = "main"                                              # Branch name for the trigger source repository
playground-cloudbuild-private = "playground-logs-private-env"               # Name of an exisitng bucket for private Cloud Build logs <private-logs-bucket>
playground-cloudbuild-public = "playground-logs-public-en"                  # Name of an exisitng bucket for public Cloud Build logs <public-logs-bucket>
```

2. Configure authentication for the Google Cloud Platform
```
gcloud init
```
```
gcloud auth application-default login
```

# Connect GitHub repository and GCP Cloud Build

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect your GitHub repository and GCP Cloud Build.

# Deploy Cloud Build triggers

1. Apply trigger dependencies

The `playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to set up Cloud Build triggers for Playground:
- Required API services
- Service accounts for Cloud Build triggers
- IAM roles for Cloud Build service accounts
```
cd playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/
```
```
# Provide an existing bucket to store Terraform state
terraform init -backend-config="bucket=<triggers-state-bucket>"
```
```
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"
```

2. Create new triggers
```
cd playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders/
```
```
# Provide an existing bucket to store Terraform state
terraform init -backend-config="bucket=<triggers-state-bucket>"
```
```
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"
```
