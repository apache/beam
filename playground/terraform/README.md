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
# Here you will find the steps for deploying BEAM Playground on your project

## Prerequisit:

### Before you will start with installation steps, please read this carefully:
1. How to create new project in Google Cloud Platform:
```
https://cloud.google.com/resource-manager/docs/creating-managing-projects
```
**Description:** * *How to create new project in Google Cloud Platform. It's strongly recommended to use new Google Cloud Project for your new projects* *

2. How to create new service account
```
https://cloud.google.com/iam/docs/creating-managing-service-accounts
```
**Description:** * *You will find the instruction "How to create Service account" for your new project* *

3. How to create a JSON key for Google Service account:
```
https://cloud.google.com/iam/docs/creating-managing-service-account-keys
```
**Description:** * *In this instruction will be explaned how to create JSON key for your service account. it will be required for the authentication.* *

4. How to create a new bucket:
```
https://cloud.google.com/storage/docs/creating-buckets
```
**Description:** * *Short instruction for bucket creation. You will need at least one bucket for file keeping* *

5. How to assign new role for service account:
```
https://cloud.google.com/iam/docs/granting-changing-revoking-access
```
**Description:** * *It will explain how to assign required roles for your service account* *

6. How to install GCloud to your Operation system
```
https://cloud.google.com/sdk/docs/install?hl=en
```
**Description:** * *GCloud - Google command line interface, which will allow you to interact with Google cloud platform thry the command line (https://en.wikipedia.org/wiki/Command-line_interface)* *


The following to be done:

* GCP project should be created
* Bucket should be created
* It is necessary to create a service account with the required roles:
   - App Engine Admin
   - App Engine Creator
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

* The following APIs ought to be activated:
   - Identity and Access Management (IAM)
   - Compute Engine API
   - App Engine Admin API
   - Cloud Resource Manager API

* Modify the following file, `playground/terraform/environment/${env_folder}/terraform.tfvars`, as necessary:
```
network_name         = "network_name"        #Enter the network name
project_id           = "project_id"          #Enter the project ID
gke_name             = "playground-backend"  #Set the GKE name
region               = "us-east1"            #Set the region
pg_location          = "us-east1-b"          #Select the location (it should be in the chosen region)
state_bucket         = "bucket_name"         #Name of bucket
bucket_examples_name = "bucket_name-example" #Enter an example bucket name
```
* Make the following modifications to the file `playground/terraform/environment/${env_folder}/state.tfbackend` file:
```
bucket               = "bucket_name"         #input bucket name (will be used for terraform tfstate file)
```
* Use the following command to export Google application credentials (allow service account authentication):
```
    export GOOGLE_APPLICATION_CREDENTIALS=`your service account json key locaton` (absolute path)
```
* Using the following command, activate the newly created service account:
```
    gcloud auth activate-service-account `full principal service account` --key-file=`your service account json key locaton` (absolute path)
```
* Install kubectl:
```
             curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" &&\
             chmod +x kubectl &&\
             mv kubectl /usr/local/bin/
```
* Install Helm:
```
             curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 &&\
             chmod 700 get_helm.sh &&\
             ./get_helm.sh
```
# Infrastructure deployment:
* To deploy the Infrastructure, use the following command (please be sure that you are in the "beam" folder):
```
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="${env}"
```
# Backend deployment:
* Sign in to the Docker registry:
```
 cat `your json key locaton` | docker login -u _json_key --password-stdin https://`chosen_region`-docker.pkg.dev
```
* Sign in to the to GKE
```
gcloud container clusters get-credentials --region `chosen_pg_location` `gke_name` --project `project_id`
```
* Creation of database indexes :
```
gcloud app deploy playground/index.yaml --project=`project_id`
```
* To deploy the backend, use the following command (Ensure you are in the "beam" folder):
```
./gradlew playground:terraform:gkebackend -Pproject_environment="${env}" -Pdocker-tag="${env}"
```
# !! The creation of certificates for backend websites will take about 20 minutes
# Frontend deployment:
* To deploy the frontend, use the following command (Ensure you are in the "beam" folder):
```
./gradlew playground:terraform:deployFrontend -Pdocker-tag="${env}" -Pproject_id=`project_id` -Pproject_environment='${env}'
```
