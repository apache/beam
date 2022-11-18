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

## Requirements:

- [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following privileges:
    - Service Account Admin
    - Storage Admin
    - Service Usage Admin
    - Cloud Build Editor
    - Security Admin
    - Service Account User
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- An existing GCP Bucket to save Terraform state - `state_bucket`
- An existing GCP Bucket to save Cloud Build logs - `logs_bucket`
- DNS name for your Playground deployment instance
- [Terraform](https://www.terraform.io/)
- [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

## 1. Setup the Google Cloud Build  for your GCP project

The `playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to setup Cloud Build for Playground:
- Required API services
- Cloud Build service account - `playground-cloudbuild-sa`
- IAM permissions for Cloud Build service account - `playground-cloudbuild-sa`

#### To execute the module:

1. Create a folder `playground/terraform/environment/{environment_name}` (further referred to as `environment_name`) to define a new environment for Playground.
2. Create `terraform.tfvars` file in the newly created `environment_name` folder.
3. Fill the `terraform.tfvars` configuration file as follows:


```console
project_id                    = "project_id"                # Your Project ID
region                        = "us-central1"               # Your GCP region name where resources will be provisioned
location                      = "us-central1-b"             # Select the deployment location from available in the specified region
cloudbuild_service_account_id = "playground-cloudbuild-sa"  # The name of Cloud Build service account
github_repository_name        = "beam"                      # The name of the GitHub repo to be connected with Cloud Build. Example: In https://github.com/example/foo is 'foo'
github_repository_owner       = "repo_owner_name"           # Owner of the GitHub repo to be connected with Cloud Build. Example: In https://github.com/example/foo is 'example'.
github_repository_branch      = "branch_name"               # The name of the GitHub repo branch to be connected with Cloud Build

network_name                  = "network_name"              # GCP VPC Network Name for Playground deployment
gke_name                      = "playground-backend"        # Playground GKE Cluster name
state_bucket                  = "bucket_name"               # GCS bucket name for Beam Playground temp files
```

4. Create and fill `state.tfbackend` configuration file as follows:


```
bucket = "bucket_name"     # The name of bucket - will be used for terraform tfstate file
```

5. Push the `environment_name` folder to your GitHub repository `github_repository_name` and branch `github_repository_branch`.

**Note:** Some regions can be prohibited for Cloud Build. Please see [Cloud Build locations](https://cloud.google.com/build/docs/locations) for supported locations.

6. Run commands:


```console
# Create a new authentication configuration for GCP Project with the created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup folder
cd playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/

# Run terraform scripts
terraform init -backend-config="../../../environment/{environment_name}/state.tfbackend"
terraform plan -var-file="../../../environment/{environment_name}/terraform.tfvars"
terraform apply -var-file="../../../environment/{environment_name}/terraform.tfvars"
```

## 2. Connect GitHub repository and Cloud Build

**Note:** Ensure correct `region` is set in [Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page before proceeding further.

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github)
to connect the GitHub repository `https://github.com/{github_repository_owner}/{github_repository_name}` and `{github_branch_name}` with Cloud Build.

## 3. Setup the Google Cloud Build triggers

The `playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders` provisions:
- Cloud Build triggers to build and deploy Beam Playground.

#### To execute the module


```
# Navigate to playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders folder
cd ../02.builders

# Run terraform scripts
terraform init -backend-config="../../../environment/{environment_name}/state.tfbackend"
terraform plan -var-file="../../../environment/{environment_name}/terraform.tfvars"
terraform apply -var-file="../../../environment/{environment_name}/terraform.tfvars"
```

## 4. Running the first Cloud Build trigger to deploy Playground infrastructure

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (Example: us-central1).
2. Open Trigger: `Playground-infrastructure-trigger`.
3. Scroll down to `Advanced` - `Substitutions variables` and Click on `+ ADD VARIABLE`.
4. Assign values for the next variables:
    - `_ENVIRONMENT_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; # *Created "environment_name"*
    - `_DNS_NAME`  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your DNS record name*
    - `_LOGS_BUCKET_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GCP logs bucket name `logs_bucket`*
5. Click `Save` and Run the trigger `Playground-infrastructure-trigger`.

6. Once Playground infrastructure has been deployed, please navigate to
   [Playground deployment README](https://github.com/apache/beam/tree/master/playground/terraform#deploy-playground-infrastructure) and execute step #2:
   `Add following DNS A records for the discovered static IP address`

## 5. Running the second Cloud Build trigger to deploy Playground to GKE

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the region (Example: us-central1).
2. Open Trigger: `Playground-to-gke-trigger`.
3. Scroll down to `Advanced` - `Substitutions variables` and Click on `+ ADD VARIABLE`.
4. Assign values for the next variables:
    - `_ENVIRONMENT_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  # *Created "environment_name"*
    - `_DNS_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  &nbsp;  &nbsp;  *# Your DNS record name*
    - `_TAG` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Tag name for your Playground container images*
    - `_GKE_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GKE cluster name (configured in terraform.tfvars)*
    - `_LOGS_BUCKET_NAME` &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; *# Your GCP logs bucket name `logs_bucket`*
5. Click `Save` and Run the trigger `Playground-to-gke-trigger`.

## 6. Validation

Once Playground has been deployed to GKE, please navigate to [Validation](https://github.com/apache/beam/tree/master/playground/terraform#validate-deployed-playground) to perform Playground deployment steps.