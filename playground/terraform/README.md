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

# Prerequisit:

* GCP project should be created
* Bucket should be created
* Service account with the following roles should be created:
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
* Following APIs should be enabled:
   - Identity and Access Management (IAM)
   - Compute Engine API
   - App Engine Admin API
   - Cloud Resource Manager API

* Make necessary changes in `playground/terraform/environment/beta/terraform.tfvars` file:
network_name         = "network_name"         #Choose network name
project_id           = "project_id"      #Input project ID
gke_name             = "playground-backend" #Define GKE name
region               = "us-east1"           #Choose region
pg_location          = "us-east1-b"         #Choose location (should be in the region)
state_bucket         = "bucket_name"          #Input bucket name
bucket_examples_name = "bucket_name-example"  #Input example bucket name

* Make necessary changes in `playground/terraform/environment/beta/state.tfbackend` file:
bucket               = "bucket_name" #input bucket name (will be used for tfstate file)

* Export GOOGLE_APPLICATION_CREDENTIALS using following command:
    export GOOGLE_APPLICATION_CREDENTIALS=`your json key locaton`

* Activate created service account using following command:
    gcloud auth activate-service-account `full principal service account` --key-file=`your json key locaton`

* Install kubectl:
             curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" &&\
             chmod +x kubectl &&\
             mv kubectl /usr/local/bin/
* Install Helm:
             curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 &&\
             chmod 700 get_helm.sh &&\
             ./get_helm.sh

# Infrastructure deployment:
* Run following command for infrastructure deployment (please be sure that you are in the "beam" folder):
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="beta"

# Backend deployment:
* Login to Docker registry:
 +cat `your json key locaton` | docker login -u _json_key --password-stdin https://`chosen_region`-docker.pkg.dev

* Login to GKE
gcloud container clusters get-credentials --region `chosen_pg_location` `gke_name` --project `project_id`

* Database index creation:
gcloud app deploy playground/index.yaml --project=`project_id`

* Please run following command for backend deployment (please be sure that you are in the "beam" folder):
./gradlew playground:terraform:gkebackend -Pproject_environment="beta" -Pdocker-tag="beta"

# !! Please wait about 20 min before frontend deployment
# Frontend deployment:
* Run following command for frontend deployment (please be sure that you are in the "beam" folder):
./gradlew playground:terraform:deployFrontend -Pdocker-tag="beta" -Pproject_id=pg-fourht -Pproject_environment='beta'
