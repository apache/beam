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

## Requirements:

- [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
    - Security Admin
    - Service Account User
    - Secret Manager Admin
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing GCS Bucket to save Terraform state for Cloud Build triggers <triggers-state-bucket>
- An existing GCS Bucket to store private Cloud Build logs <private-logs-bucket>
- An existing GCS Bucket to store public Cloud Build logs <public-logs-bucket>
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## 1. Set up the Google Cloud Build for your GCP project

First provide the variables by creating a `common.tfvars`
```
beam/playground/terraform/infrastructure/cloudbuild-manual-setup/common.tfvars 
```
And put the following:
```
playground_deploy_sa = "DEPLOY_SA_NAME" # SA name used for Deploy trigger
playground_update_sa = "UPDATE_SA_NAME" # SA name used for Update trigger
playground_ci_sa = "CI_SA_NAME" # SA name used for CI trigger
playground_cd_sa = "CD_SA_NAME" # SA name used for CD trigger
project_id = "PROJECT_ID" # ID of the project used
playground_environment_name = "environment" # Name of the environment. Used for prefixing (dev- stag- prod- etc.)
playground_dns_name = "fqdm.playground.zone" # FQDN used for Playground deployment
image_tag = "tag" # Container image tag to build
playground_region = "us-central1" # GCP Region to deploy in
playground_zone = "us-central1-a" # GCP Zone to deploy in
skip_appengine_deploy = false # Workaround for Appengine issue. Appengine can only be deployed once so subsequent runs need to set this to true
webhook_trigger_secret_id = "SECRET_ID" # Secret ID for webhook
gh_pat_secret_id = "PAT_SECRET_ID" # Secret ID with github PAT
data_for_github_pat_secret = "PAT" # Actual Github PAT
trigger_source_repo = "https://github.com/beamplayground/deploy-workaround" # Repo used as a workaround
terraform_source_repo = "https://github.com/apache/beam" # Repo from which terraform code is fetched 
terraform_source_branch = "master" # Branch from which terraform code is fetched
state_bucket = "BUCKET_NAME" # State bucket to preseve environment state
data_for_cicd_webhook_secret = "secret_sting"  # Secret used when creating the Github webhook 
```

Please make sure you change the values. 

The `playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to set up Cloud Build triggers for Playground:
- Required API services
- Service accounts for Cloud Build triggers
- IAM roles for Cloud Build service accounts

#### To execute the module:

**Note:**  Please see [Cloud Build locations](https://cloud.google.com/build/docs/locations) for the list of all supported locations.

1. Run commands:


```console
# Set environment variable for state bucket
export STATE_BUCKET="state-bucket"

# Create a new authentication configuration for GCP Project with the created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup directory
cd playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/

# Run terraform commands
terraform init -backend-config="bucket=$STATE_BUCKET"
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file=../common.tfvars"
```
## 3. Connect your GitHub repository and GCP Cloud Build

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect your GitHub repository and GCP Cloud Build.

## 4. Set up the Google Cloud Build triggers

The `playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build triggers to build and deploy Beam Playground, update Beam Playground, and run CI/CD checks.

#### To execute the module

```
# Navigate to playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders directory
cd ../02.builders

# Run terraform commands
terraform init -backend-config="bucket=$STATE_BUCKET"
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="$BEAM_ROOT/playground/terraform/environment/$ENVIRONMENT_NAME/common.tfvars"
```

**Note:**  you will have to provide values for multiple variables required for setup of triggers

## 5. Copy inline yaml scripts into cloud build triggers