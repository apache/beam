<!--
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
# The Tour of Beam deployment on GCP
This guide provides instructions on how to deploy the Tour of Beam environment on Google Cloud Platform (GCP) and Firebase environment. Before starting the deployment, ensure that you have the following prerequisites in place:

## Prerequisites:

1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
   Ensure that the account has at least following privileges:
   - Cloud Datastore Owner
   - Create Service Accounts
   - Security Admin
   - Service Account User
   - Service Usage Admin
   - Storage Admin
   - Kubernetes Engine Cluster Viewer

3. [Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) for saving deployment state

4. An OS with the following software installed:

* [Java](https://adoptopenjdk.net/)
* [Flutter (3.7.3 >)](https://docs.flutter.dev/get-started/install)
* [Dart SDK (2.19.2)](https://dart.dev/get-dart)
* [Firebase-tools CLI](https://www.npmjs.com/package/firebase-tools)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication plugin](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
* [Go](https://go.dev/doc/install)

5. Beam Playground environment (Existing GKE Cluster will be required particularly)

6. Apache Beam Git repository cloned locally

# Prepare deployment configuration:
 `common.tfvars` located in `learning/tour-of-beam/terraform` to define variables specific to an environment (e.g., prod, test, staging). Follow the steps below to prepare the deployment configuration:<br>
1. Create a `common.tfvars` and `state.tfbackend` files in terraform directory `learning/tour-of-beam/terraform`:

* Populate `common.tfvars` with next variables:
```
region                  = "gcp_region"                # Your GCP resources region
```
* `state.tfbackend` with:
```
bucket = "bucket_name"          # Your created bucket name for terraform tfstate file
```
2. Configure authentication for the Google Cloud Platform (GCP)
```
gcloud init
```
```
gcloud auth application-default login
```

3. Configure authentication in the GCP Docker registry:
```
 gcloud auth configure-docker `chosen_region`-docker.pkg.dev
```
4. And the authentication in GCP Google Kubernetes Engine:
```
gcloud container clusters get-credentials --region `chosen_gke_zone` `gke_name` --project `project_id`
```

# Deploy the Tour of Beam Backend Infrastructure:

5. Run the following command from the top-level repository folder ("beam") to deploy the Tour of Beam Backend infrastructure:
```
./gradlew learning:tour-of-beam:terraform:InitBackend -Pgcloud_account=`gcloud config get-value core/account` -Pproject_environment="environment_name" -Pproject_id="gcp-project-id"
```
Where:
- **project_environment** - environment name
- **project_id** - name of your GCP Project ID

# Deploy the Tour of Beam Frontend Infrastructure:

6. Run the following command and follow the instructions to configure authentication for Firebase:
```
firebase login --no-localhost
```

7. Run the following command from the top-level repository folder ("beam") to deploy the Tour of Beam Frontend infrastructure:
```
./gradlew learning:tour-of-beam:terraform:InitFrontend -Pproject_environment="environment_name" -Pproject_id="gcp-project-id" -Pdns-name="playground-dns-name" -Pregion="gcp-region" -Pwebapp_id="firebase_webapp_name"
```
Where:
- **project_environment** - environment name
- **project_id** - name of your GCP Project ID
- **dns-name** - DNS name reserved for Beam Playground
- **region** - name of your GCP Resources region
- **webapp_id** - name of your Firebase Web Application that will be created (example: Tour-of-Beam-Web-App)

# Validate the deployment of the Tour of Beam:
8. Open the Tour of Beam webpage in a web browser (Hosting URL will be provided in terminal output) to ensure that deployment has been successfully completed.

Example:
```
✔  Deploy complete!

Project Console: https://console.firebase.google.com/project/some-gcp-project-id/overview
Hosting URL: https://some-gcp-project-id.web.app
```