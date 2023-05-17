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

# Tour of Beam Cloud Build Setup

This directory organizes Infrastructure-as-Code to provision dependent resources and set up Cloud Build for automated Tour of Beam Backend Infrastructure provisioning.

## Requirements:

1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
  Ensure that the account has at least the following [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):

   - Cloud Datastore Owner
   - Create Service Accounts
   - Security Admin
   - Service Account User
   - Service Usage Admin
   - Storage Admin
   - Kubernetes Engine Cluster Viewer


3. An OS with the following software installed:

* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication plugin](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
DEV NOTE: GCP Cloud shell can be used for deployment. It has all required software pre-installed.

4. Additionaly for manual Frontend deployment you will need 
* [Flutter (3.7.3 >)](https://docs.flutter.dev/get-started/install)
* [Dart SDK (2.19.2)](https://dart.dev/get-dart)
* [Firebase-tools CLI](https://www.npmjs.com/package/firebase-tools)

5. DNS name for your Playground deployment instance ?

6. OS with installed software listed below:
- [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- [Terraform](https://www.terraform.io/)

7. [Apache Beam GitHub](https://github.com/apache/beam) repository cloned locally

8. [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) for CI trigger

9. Existing Beam Playground environment/infrastructure in same GCP Project



## 1. Set up the Google Cloud Build for your GCP project

The `beam/learning/tour-of-beam/cloudbuild/01.setup` provisions dependencies required to set up Cloud Build for Tour of Beam:
- Required API services
- Cloud Build service account
- IAM roles for Cloud Build service account

#### To execute the module:

1. Run commands:

```console

# Create a new authentication configuration for GCP Project with the created user account
gcloud init

# Command imports new user account credentials into Application Default Credentials
gcloud auth application-default login

# Navigate to 01.setup directory
cd beam/learning/tour-of-beam/cloudbuild/01.setup

# Run terraform commands
terraform init -backend-config="bucket=$STATE_BUCKET"
terraform apply -var="project_id=$(gcloud config get-value project)"
```

## 2. Connect default (https://github.com/beamplayground/deploy-workaround) GitHub repository with GCP Cloud Build

Follow [Connect to a GitHub repository](https://cloud.google.com/build/docs/automating-builds/github/connect-repo-github) to connect GitHub repository with GCP Cloud Build.

## 3. Set up the Google Cloud Build triggers

The `beam/learning/tour-of-beam/cloudbuild/02.builders` provisions:
- Cloud Build triggers to build and deploy Tour of Beam backend infrastructure

#### To execute the module

```
# Navigate to beam/learning/tour-of-beam/cloudbuild/02.builders directory
cd ../02.builders

# Run terraform commands and provide required values
terraform init -backend-config="bucket=$STATE_BUCKET"

TODO: add preparing of tfvars file

terraform apply -var="project_id=$(gcloud config get-value project)" -var-file="../common.tfvars"


```

## 4. Run Cloud Build trigger to deploy Tour of Beam backend infrastructure

1. Navigate to [GCP Console Cloud Build Triggers](https://console.cloud.google.com/cloud-build/triggers) page. Choose the global region.
2. Open Trigger: `Tourofbeam-backend-infra-trigger`.
3. Scroll down to `Source` - `Repository` to ensure that Default GitHub repository is connected.
   - Click on drop-down menu and press `CONNECT NEW REPOSITORY` in case it was not automatically connected.
4. Click `Save` and Run the trigger.

## 5. Validate Tour of Beam backend infrastructure deployment

1. Navigate to Cloud Functions service in GCP.
2. Check if there are cloud functions with prefix of environment (e.g. prod, test) in their names.
