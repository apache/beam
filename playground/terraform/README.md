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
# Playground deployment on GCP

## Prerequisites:

### Following items need to be setup for Playground deployment on GCP:
1. [GCP project](https://cloud.google.com/)

2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_

***Google Cloud preparation steps:***
After creating or identified an existing project for deployment, please be sure that your account has at least following privileges:
   - App Engine Admin
   - App Engine Creator -??
   - Artifact Registry Administrator
   - Cloud Memorystore Redis Admin
   - Compute Admin
   - Create Service Accounts
   - Kubernetes Engine Admin
   - Quota Administrator
   - Role Administrator
   - Security Admin
   - Service Account User
   - Storage Admin
   - Cloud Datastore Index Admin

3. [Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) for saving deployment state

4. DNS name for your Playground deployment instance

***Development environment preparation steps:***

During the Operation system preparation steps, you will need to install Java, Kubernetes, HELM, Docker, Terraform, gcloud CLI

* [Java](https://adoptopenjdk.net/)
* [Kubernetes Command Line Interface](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
* [HELM](https://helm.sh/docs/intro/install/)
* [Docker](https://docs.docker.com/engine/install/)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)

After Development environment preparation steps completed, navigate to the "beam" folder in your development environment.

# Deployment environment:
Playground uses `terraform.tfvars` located in `playground/terraform/environment/environment_name` to define all infrastructure environment (like staging, production, etc.). Create a folder `environment_name` and place all configuration files inside to add a new environment.

* `terraform.tfvars` environment variables:
```
project_id           = "project_id"          #GCP Project ID
network_name         = "network_name"        #GCP VPC Network Name for Playground deployment
gke_name             = "playground-backend"  #Playground GKE Cluster name
region               = "us-east1"            #Set the deployment region
location             = "us-east1-b"          #Select the deployment location from available in the specified region
state_bucket         = "bucket_name"         #GCS bucket name for Beam Playground temp files, [terraform state file] (https://cloud.google.com/docs/terraform/resource-management/store-state)
bucket_examples_name = "bucket_name-example" #GCS bucket name for Playground examples storage
dnsname              = "your-dns-name."      #Variable for Playground DNS name (a dot at the end is required)
```
* `state.tfbackend` environment variables:
```
bucket               = "bucket_name"         #input bucket name - will be used for terraform tfstate file
```
Then, configure authentication for the Google Cloud Platform:

* The following commands allows us to configure account and project
```
gcloud init
```
```
gcloud auth application-default login
```
# Infrastructure deployment:
* To deploy the Payground infrastructure, use the following command (please be sure that you are in the "beam" folder):
```
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="env" (env - folder name which you created for configuration files)
```
# Playground deployment:
Afrer successfully deploying Playground infrastrcuture in the previous step, authenticate on Docker and Google Kubernetes Engine
The following command will authenticate us in the Docker registry
```
 gcloud auth configure-docker `chosen_region`-docker.pkg.dev
```
* The following command will authenticate in GKE
```
gcloud container clusters get-credentials --region `chosen_location` `gke_name` --project `project_id`
```
* We need to create database indexes for Beam Playground examples by the following command:
```
gcloud app deploy playground/index.yaml --project=`project_id`
```
Please add NS records from your [Cloud DNS](https://cloud.google.com/dns/docs/records) (project_id>Cloud DNS>playground>NS records) to your domain registrator
This step completes the configuration of the deployment environment. 

To deploye the Beam Playground to the configured envrionment, please execute the following command (Ensure you are in the "beam" folder):
```
./gradlew playground:terraform:gkebackend -Pproject_environment="env" -Pdocker-tag="tag" (env - folder name which you created for configuration files, tag - image tag for backend)
```
During script execution, a Google managed certificate will be created (allow time for [provisioning the certificate](https://cloud.google.com/load-balancing/docs/ssl-certificates/google-managed-certs)).
