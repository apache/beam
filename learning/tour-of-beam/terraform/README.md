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
# Tour of Beam deployment on GCP
This guide provides instructions on how to deploy the Tour of Beam environment on Google Cloud Platform (GCP) and Firebase environment. Before starting the deployment, ensure that you have the following prerequisites in place:

## Prerequisites:

### Following items need to be setup for Tour of Beam deployment on GCP:
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

4. DNS name for your Tour of Beam deployment instance (or use subdomain)

5. OS with installed software listed below:

* [Java](https://adoptopenjdk.net/)
* [NodeJS & npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm/)
* [Flutter (3.7.3 >)](https://docs.flutter.dev/get-started/install)
* [Dart SDK (2.19.2)](https://dart.dev/get-dart)
* [Firebase-tools CLI](https://www.npmjs.com/package/firebase-tools)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication plugin](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
* [Go](https://go.dev/doc/install)

6. Apache Beam Git repository cloned locally

# Prepare deployment configuration:
Tour of Beam backend uses `terraform.tfvars` located in `learning/tour-of-beam/terraform/environment/environment_name/` to define variables specific to an environment (e.g., prod, test, staging).<br>
1. Create a folder (further referred as `environment_name`) to define a new environment and place configuration files into it:

* `terraform.tfvars` environment variables:
```
project_id = "gcp_project_id"                   # Your GCP Project ID
cloudfunctions_bucket = "gcs_bucket_name"       # Name of bucket that will be created for cloud functions source code (Note: has to be globally unique)
region = "gcp_region"                           # Your GCP resources region

```
* `state.tfbackend` environment variables:
```
bucket = "bucket_name"                          # Your created bucket name for terraform tfstate file
```
2. Configure authentication for the Google Cloud Platform
```
gcloud init
```
```
gcloud auth application-default login
```

3. Run the following command to authenticate in the Docker registry:
```
 gcloud auth configure-docker `chosen_region`-docker.pkg.dev
```
4. Run the following command to authenticate in GKE:
```
gcloud container clusters get-credentials --region `chosen_gke_zone` `gke_name` --project `project_id`
```

# Deploy Tour of Beam Backend:

5. Run the command below from the top level repository folder ("beam") to deploy the Tour of Beam Backend infrastructure:
```
./gradlew learning:tour-of-beam:terraform:InitBackend -Pproject_environment="environment_name" -Pproject_id="gcp-project-id"
```
Where:
- **project_environment** - environment name
- **project_id** - name of your GCP Project ID

# Deploy Tour of Beam Frontend:

6. Run the command below and follow instructions to configure authentication for the Firebase
```
firebase login --no-localhost
```

7. Run the following command from the top level repository folder ("beam") to deploy the Tour of Beam Frontend infrastructure:
```
./gradlew learning:tour-of-beam:terraform:InitFrontend -Pproject_environment="environment_name" -Pproject_id="gcp-project-id" -Pdns-name="playground-dns-name" -Pregion="gcp-region" -Pwebapp_id="firebase_webapp_name" 
```
Where:
- **project_environment** - environment name
- **project_id** - name of your GCP Project ID
- **dns-name** - DNS name reserved for Beam Playground
- **region** - name of your GCP Resources region
- **webapp_id** - name of your Firebase Web Application that will be created (example: Tour-of-Beam-Web-App)

# Validate deployed Tour of Beam:
8. Open Tour of Beam frontend webpage in a web browser (Hosting URL will be provided in output once script is finished) to ensure that web page is available

Example:
```
✔  Deploy complete!

Project Console: https://console.firebase.google.com/project/some-gcp-project-id/overview
Hosting URL: https://some-gcp-project-id.web.app
```