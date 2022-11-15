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

This directory organizes Infrastructure-as-Code to provision dependent resources and setup Cloud Build for Beam Playground.

## Requirements:

- [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least following privileges:
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- Two GCP Buckets:
  1. An existing Google Cloud Storage Bucket to save Terraform state - `state_bucket`
  2. An existing GCP Bucket to save Cloud build logs - `cloudbuild_Logs_bucket`
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## 1. Setup the Google Cloud Build  for your GCP project

The `playground/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to setup Cloud Build for Playground:
- Required API services
- Cloud Build service account
- IAM permissions for Cloud Build service account

#### To execute the module:

1. Create configuration file terraform.tfvars file by path: `playground/terraform/cloudbuild-manual-setup/01.setup/terraform.tfvars`
and provide values as per example below:

```
project                       = "sandbox-playground-001"    # Your Project ID
region                        = "us-central1"               # Your GCP region name where resources will be provisioned
cloudbuild_service_account_id = "terraform-cloudbuild"      # Name of SA to be used by Cloud Build to deploy Playground
```

2. Run commands:

```console
# Creates new authentication configuration to GCP Project
# Using user account in Requirements
gcloud init

# Acquires new user account credentials to use for Application Default Credentials
gcloud auth application-default login

# Navigate to ../01.setup folder
cd beam/playground/terraform/cloudbuild-manual-setup/01.setup

# Run terraform scripts
terraform init -backend-config="bucket="YOUR_BUCKET_FOR_TF_STATE"
terraform plan -var-file="../terraform.tfvars"
terraform apply -var-file="../terraform.tfvars"

```

## 2. Connect GitHub repository and Cloud Build
Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect GitHub repository and Cloud Build.

## 3. Setup the Google Cloud Build triggers

The `playground/infrastructure/cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build trigger to build and deploy Beam Playground.

#### Execute the module

1. Append to `terraform.tfvars` file by path: `playground/terraform/cloudbuild-manual-setup/01.setup/terraform.tfvars`
next variables:

```
github_repository_name        = "beam"                      # The name of the GitHub repo. Example: In https://github.com/example/foo is 'foo'
github_repository_owner       = "ruslan-ikhsan"             # Owner of the GitHub repo. Example: In https://github.com/example/foo is 'example'.
github_repository_branch      = "cloudbuild+playground"     # The name of GitHub repo branch
```

Final version of `terraform.tfvars` will look like:

```
project                       = "sandbox-playground-001"    # Your Project ID
region                        = "us-central1"               # Your GCP region name where resources will be provisioned
cloudbuild_service_account_id = "terraform-cloudbuild"      # Name of SA to be used by Cloud Build to deploy Playground
github_repository_name        = "beam"                      # The name of the GitHub repo. Example: In https://github.com/example/foo is 'foo'
github_repository_owner       = "ruslan-ikhsan"             # Owner of the GitHub repo. Example: In https://github.com/example/foo is 'example'.
github_repository_branch      = "cloudbuild+playground"     # The name of GitHub repo branch
```

2. Run:

```
# Navigate to ../02.builders folder
cd beam/playground/terraform/cloudbuild-manual-setup/02.builders
# Run terraform scripts
terraform init -backend-config="bucket="YOUR_BUCKET_FOR_TF_STATE"
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

## 4. Running First Cloud Build Trigger: "Playground-infrastructure-trigger"

1. Perform steps described in [Prepare deployment configuration](https://github.com/apache/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#prepare-deployment-configuration).

2. Change the variables in `SUBSTITUTIONS` block in first cloud build config file `playground/infrastructure/cloudbuild/cloudbuild_pg_infra.yaml`.
3. Navigate to GCP Console. Open Cloud Build page -> Triggers. Sort by relevant region and click `RUN` for `Playground-infrastructure-trigger`.

4. Once Playground infrastructure has been deployed, please navigate to
[Playground deployment README](https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#deploy-playground-infrastructure) and execute step #2:
`Add following DNS A records for the discovered static IP address`

## 5. Running Second Cloud Build Trigger: "Playground-to-gke-trigger"

1. Change variables in `SUBSTITUTIONS` block in second cloud build config file `playground/infrastructure/cloudbuild/cloudbuild_pg_to_gke.yaml`
2. Navigate to GCP Console. Open Cloud Build page -> Triggers. Sort by relevant region and click `RUN` for `Playground-to-gke-trigger`.

## 6. Validation

Once Playground has been deployed to GKE, please navigate to [Validation](https://github.com/akvelon/beam/tree/Infra%2Bplayground-in-gke/playground/terraform#validate-deployed-playground) to perform Playground deployment steps.