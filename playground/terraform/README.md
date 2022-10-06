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

## Prerequisite:

### Before you will start with the installation steps, please read this carefully:
1. How to create a new project in Google Cloud Platform:
```
https://cloud.google.com/resource-manager/docs/creating-managing-projects
```
**Description:** * *How to create a new project in Google Cloud Platform. It's strongly recommended to use the new Google Cloud Project for your new projects* *

2. How to create a new service account
```
https://cloud.google.com/iam/docs/creating-managing-service-accounts
```
**Description:** * *You will find the instruction "How to create Service account" for your new project* *

3. How to create a JSON key for a Google Service account:
```
https://cloud.google.com/iam/docs/creating-managing-service-account-keys
```
**Description:** * *In this instruction will be explained how to create a JSON key for your service account. it will be required for authentication.* *

4. How to create a new bucket:
```
https://cloud.google.com/storage/docs/creating-buckets
```
**Description:** * *Short instruction for bucket creation. You will need at least one bucket for file keeping* *

5. How to assign a new role for the service account:
```
https://cloud.google.com/iam/docs/granting-changing-revoking-access
```
**Description:** * *It will explain how to assign required roles for your service account* *

6. How to install GCloud to your Operation system
```
https://cloud.google.com/sdk/docs/install?hl=en
```
**Description:** * *GCloud - Google command line interface, which will allow you to interact with the Google cloud platform thru the command line (https://en.wikipedia.org/wiki/Command-line_interface)* *

7. What are Google Cloud Platform APIs
```
https://cloud.google.com/apis
```
**Description:** * *Short description of all Google Platform APIs* *

8. How to Enable Google Cloud Platform API
```
https://cloud.google.com/endpoints/docs/openapi/enable-api
```
**Description:** * *Steps to enable APIs in Google Cloud Platform* *

9. Google Cloud Platform naming policy
```
https://cloud.google.com/compute/docs/naming-resources
```
**Description:** * *Describes the naming convention for Compute Engine resources.* *
10. What is Docker
```
https://docs.docker.com/get-started/overview/
```
**Description:** * *What is Docker and how to use it* *

***Google Cloud preparation steps:***
When you create a new project, you must add a service account with a JSON key and a bucket according to the instructions above. Also, according to the instructions, configure the roles for the service account:
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

These Google APIs will be needed for the BEAM Playground
   - **Identity and Access Management (IAM)** - Manages identity and access control for Google Cloud Platform resources, including the creation of service accounts, which you can use to authenticate to Google and make API calls
   - **Compute Engine API** - Creates and runs Virtual Machines on Google Cloud
   - **App Engine Admin API** - Provisions and manages App Engine applications
   - **Cloud Resource Manager API** - Creates, reads, and updates metadata for Google Cloud Platform resource containers
You may find the instruction "How to enable Google Cloud Platform API" above

***Operation System preparation steps:***

During the Operation system preparation steps, you will need to install Java, HELM, GIT, Docker, GCloud, Terraform, Kubernetes command line interface

1. How to install Java:
* [This link](https://adoptopenjdk.net/) will provide you will all necessary instructions for Windows and Linux operation systems

2. How to install Kubernetes Command Line Interface
* [Linux link](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/) instructions for Linux operation systems
* [Windows link](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows/) instructions for Windows operation systems

3. How to install HELM
* [HELM link](https://helm.sh/docs/intro/install/) instructions for Windows and Linux operation systems

4. How to install GIT
* [Git link](https://git-scm.com/download/win) instructions for Windows operation systems
* [Git Link](https://git-scm.com/download/linux) instructions for Linux operation systems

5. How to install Docker
* [Docker link](https://docs.docker.com/engine/install/) instructions for Windows and Linux operation systems

6. How to install GCloud
* [Gcloud link](https://cloud.google.com/sdk/docs/install#linux) instructions for Windows and Linux operation systems

7. How to install Terraform
* [Terraform link](https://www.terraform.io/downloads)

After installations of the required packages will be completed, you will need to download ***BEAM Playground*** from the repository. Please open the terminal and execute the following command:

`git clone 'url_to_github_project_with_beam_playground'`

Once downloading is completed you will find the "beam" folder on your disk (you can execute the `ls -la` command for Linux, or the `dir` command for Windows). Open that folder

It is possible to configure BEAM to work with the different versions of the environment (like staging, production, etc.), you will need to create a special folder in `playground/terraform/environment` and put all configuration files inside:
* File name - `terraform.tfvars`, items inside:
```
network_name         = "network_name"        #Enter the network name - you can choose any name for the network according to the naming policy above
project_id           = "project_id"          #Enter the project ID - ID of created project
gke_name             = "playground-backend"  #Set the GKE name - you can choose any name for Google Kubernetes Engine according to the naming policy above
region               = "us-east1"            #Set the region - preferred region according to your needs
pg_location          = "us-east1-b"          #Select the location - location should be in region you set before
state_bucket         = "bucket_name"         #Name of bucket - Google Cloud bucket where BEAM Playground will put temp files, [terraform state file] (https://spacelift.io/blog/terraform-state)
bucket_examples_name = "bucket_name-example" #Enter an example bucket name - bucket for some build-in examples for BEAM Playground
```
* File name - `state.tfbackend`, item inside:
```
bucket               = "bucket_name"         #input bucket name - will be used for terraform tfstate file
```
Then, let's configure authentication for the Google Cloud Platform:

* The following command allows us to authenticate using JSON key file
```
    export GOOGLE_APPLICATION_CREDENTIALS=`your service account JSON key location` (absolute path)
```
* Using the following command, we will activate the newly created service account:
```
    gcloud auth activate-service-account `full principal service account` --key-file=`your service account JSON key location` (absolute path)
```

# Infrastructure deployment:
* To deploy the Infrastructure, use the following command (please be sure that you are in the "beam" folder):
```
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="env" (env - folder name which you created for configuration files)
```
# Backend deployment:
Once the script was executed successfully, you will need to authenticate on Docker and Google Kubernetes Engine
The following command will authorize us in the Docker registry
```
 cat `your service account json key locaton` | docker login -u _json_key --password-stdin https://`chosen_region`-docker.pkg.dev
```
* The following command will authenticate us in GKE
```
gcloud container clusters get-credentials --region `chosen_pg_location` `gke_name` --project `project_id`
```
* We need to create database indexes for BEAM playground examples by the following command:
```
gcloud app deploy playground/index.yaml --project=`project_id`
```
That's all, the configuration of the environment has been completed. For deploying the backend part to the Google cloud Kubernetes engine, please execute the following command (Ensure you are in the "beam" folder):
```
./gradlew playground:terraform:gkebackend -Pproject_environment="env" -Pdocker-tag="tag" (env - folder name which you created for configuration files, tag - image tag for backend)
```
During script execution, a google managed certificate will be created. The provisioning process could take up to 20 minutes

# Frontend deployment:
* To deploy the frontend, use the following command (Ensure you are in the "beam" folder):
```
./gradlew playground:terraform:deployFrontend -Pdocker-tag="env" -Pproject_id=`project_id` -Pproject_environment='tag'
```
