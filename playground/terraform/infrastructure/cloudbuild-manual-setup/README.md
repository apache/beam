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
- Cloud Build public logs: \<public-logs-bucket\>. Don't enforce public access prevention on this bucket.

4. DNS name for your Playground deployment instance

5. OS with installed software listed below:
- [Terraform](https://www.terraform.io/downloads)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)

6. [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) for CI trigger

# Prepare deployment configuration

1. Generate a Terraform variable file called `beam/playground/terraform/infrastructure/cloudbuild-manual-setup/common.tfvars`. Place the values listed below into the file, adjusting them as needed:
```
playground_deploy_sa = "pg-cb-deploy"                                       # Service account for Initialize-Playground-environment trigger
playground_update_sa = "pg-cb-update"                                       # Service account for Deploy-Update-Playground-environment trigger
playground_ci_sa = "pg-cb-ci"                                               # SA name used for CI trigger
playground_cd_sa = "pg-cb-cd"                                               # SA name used for CD trigger
project_id = "<PROJECT_ID>"                                                 # GCP Project ID
webhook_trigger_secret_id = "playground-cicd-webhook"                       # Secret ID for webhook
data_for_cicd_webhook_secret = "secret_string"                              # Secret used when creating the GitHub webhook
gh_pat_secret_id = "playground-github-pat-ci"                               # Secret ID with GitHub PAT
data_for_github_pat_secret = "github_pat_*"                                 # Actual GitHub PAT
trigger_source_repo = "https://github.com/beamplayground/deploy-workaround" # Trigger source repository. The repository must be connected to Cloud Build
trigger_source_branch = "main"                                              # Branch name for the trigger source repository
cloudbuild_bucket_private = "<private-logs-bucket>"                         # Name of an existing bucket for private Cloud Build logs
cloudbuild_bucket_public = "<public-logs-bucket>"                           # Name of an existing bucket for public Cloud Build logs
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

The `beam/playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup` provisions dependencies required to set up Cloud Build triggers for Playground:
- Required API services
- Service accounts for Cloud Build triggers
- IAM roles for Cloud Build service accounts
```
cd beam/playground/terraform/infrastructure/cloudbuild-manual-setup/01.setup/
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
cd beam/playground/terraform/infrastructure/cloudbuild-manual-setup/02.builders/
```
```
# Provide an existing bucket to store Terraform state
terraform init -backend-config="bucket=<triggers-state-bucket>"
```
```
terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"
```

# Deploy Playgorund environment from Cloud Build triggers:
1. Run "Initialize-Playground-environment" trigger
2. Run "Deploy-Update-Playground-environment" trigger when previous job successfully completed
This trigger can be restarted to rebuild GKE cluster container images from _REPO_NAME repository _BRANCH_NAME branch.

Each of these steps provides substitutions to customize Playgorund environment deployment:
```
_REPO_NAME - URL to GitHub repository to deploy Playground environment (e.g. "https://github.com/apache/beam")
_BRANCH_NAME - Branch name for the _REPO_NAME repository (e.g. "master")
_ENVIRONMENT_NAME - Environment name used to generate GCP resource names for Datastore database, GKE cluster, network, Redis instance, etc. (e.g. "prod")
_GKE_MACHINE_TYPE - Machines for GKE cluster nodes. (e.g. "e2-standard-8" for a test environment, "c2-standard-16" for a production environment)
_MAX_COUNT - Number of GKE cluster nodes to scale up to
_MIN_COUNT - Minimum number of GKE cluster nodes. At least 2 nodes required
_PLAYGROUND_REGION - GCP Region to deploy Playground resources. See [Cloud Build locations](https://cloud.google.com/build/docs/locations) for the list of supported locations. (e.g. "us-central1")
_PLAYGROUND_ZONE - GCP Zone (location) to deploy Playground resources (e.g. "us-central1-a")
_SKIP_APPENGINE_DEPLOY - Set to "true" if App Engine API has already been enabled in the GCP project. Set "false" otherwise
_STATE_BUCKET - Provide a unique name for a bucket to store Terraform state file for the Playground environment. Bucket is created for you
_DNS_NAME - FQDN to access Playgorund. (e.g "play.beam.apache.org")
_SDK_TAG - BEAM SDK version (e.g. "2.44.0")
_CONTAINER_TAG - Tag for Docker containers created in Artifact Registry (e.g. "prod-1.0")
```

3. Run "Playground-CD-stable-manual" trigger to deploy Playground examples.
Provide values for substitutions:
```
_DATASTORE_NAMESPACE - Datastore namespace to store examples. Equal to "playground-<environment>", where <environment> is the value for  _ENVIRONMENT_NAME substitution from the previous step.
_DNS_NAME - FQDN to access Playgorund. Equal to _DNS_NAME substitution from the previous step.  (e.g "play.beam.apache.org")
_MERGE_COMMIT - Source Git branch or commit hash to deploy examples from. Examples are always deployed from [Apache/Beam](https://github.com/apache/beam) repository.  (e.g. "master")
_ORIGIN - Examples origin to search in _SUBDIRS
_SDKS - List of supported SDKs to deploy to the Playground environment. (e.g. "java go python")
_SUBDIRS - List of paths relative to Apache Beam repository root folder to search examples (e.g. "./learning/katas ./examples ./sdks")
_BEAM_CONCURRENCY - Number of examples that can be executed simultaneously during validation. (e.g. "4")
```
