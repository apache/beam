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
This guide shows you how to deploy full Playground environment on Google Cloud Platform (GCP) environment.
Alternatively, you can automate Playground environment deployment with Cloud Build as described in [readme](infrastructure/cloudbuild-manual-setup/README.md).

## Prerequisites:

### Following items need to be setup for Playground deployment on GCP:
1. [GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)

2. [GCP User account](https://cloud.google.com/appengine/docs/standard/access-control?tab=python) _(Note: You will find the instruction "How to create User account" for your new project)_<br>
Ensure that the account has at least following privileges:
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

3. [Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) for saving deployment state

4. DNS name for your Playground deployment instance

5. OS with installed software listed below:

* [Java](https://adoptopenjdk.net/)
* [Kubernetes Command Line Interface](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
* [HELM](https://helm.sh/docs/intro/install/)
* [Docker](https://docs.docker.com/engine/install/)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)

6. Apache Beam Git repository cloned locally

# Prepare deployment configuration:
Playground uses `terraform.tfvars` located in `playground/terraform/environment/environment_name` to define variables specific to an environment (e.g., prod, test, staging).<br>
1. Create a folder (further referred as `environment_name`) to define a new environment and place configuration files into it:

* `terraform.tfvars` environment variables:
```
project_id           = "project_id"          #GCP Project ID
network_name         = "network_name"        #GCP VPC Network Name for Playground deployment
gke_name             = "playground-backend"  #Playground GKE Cluster name
region               = "us-east1"            #Set the deployment region
location             = "us-east1-b"          #Select the deployment location from available in the specified region
state_bucket         = "bucket_name"         #GCS bucket name for Beam Playground temp files
redis_name           = "playground_redis"    #Choose the name for redis instance
min_count            = 2                     #Min node count for GKE cluster
max_count            = 6                     #Max node count for GKE cluster

```
* `state.tfbackend` environment variables:
```
bucket               = "bucket_name"         #input bucket name - will be used for terraform tfstate file
```
2. Configure authentication for the Google Cloud Platform
```
gcloud init
```
```
gcloud auth application-default login
```
# Deploy Playground infrastructure:
1. Start the following command from the top level repository folder ("beam") to deploy the Payground infrastructure:
```
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="environment_name" -Pdns-name="playground.zone"
```
Where playground.zone - chosen DNS for Playground

2. Find a Static IP in your GCP project>VPC Network>IP Addresses>pg-static-ip
<br>Add following DNS A records for the discovered static IP address:
```
java.playground.zone
python.playground.zone
scio.playground.zone
go.playground.zone
router.playground.zone
playground.zone
```
Where "playground.zone" is the registered DNS zone<br>
[More about DNS zone registration](https://domains.google/get-started/domain-search/)<br>
[More about A records in DNS](https://support.google.com/a/answer/2579934?hl=en)

# Deploy Playground to Kubernetes:

1. Run the following command to authenticate in the Docker registry:
```
gcloud auth configure-docker <chosen_region>-docker.pkg.dev
```
2. Run the following command to authenticate in GKE:
```
gcloud container clusters get-credentials --region <chosen_location> <gke_name> --project <project_id>
```
Start the following command from the top level repository folder ("beam") to deploy the Payground infrastructure:
```
./gradlew playground:terraform:gkebackend -Pproject_environment="environment_name" -Pdocker-tag="tag" \
  -Pdns-name="playground.zone" -Psdk-tag=2.44.0 \
   -Pdocker-repository-root="<chosen_region>-docker.pkg.dev/<project_id>/playground-repository"
```
Where tag - image tag for backend, playground.zone - chosen DNS for Playground, Psdk-tag - current BEAM version

During script execution, a Google managed certificate will be created. [Provisioning might take up to 60 minutes](https://cloud.google.com/load-balancing/docs/ssl-certificates/google-managed-certs).

# Validate deployed Playground:
1. Run "helm list" command in the console to ensure that status is "deployed":
```
NAME            NAMESPACE  REVISION        UPDATED         STATUS          CHART                          APP VERSION
playground      default       1            your time      deployed        playground-2.44.0-SNAPSHOT         1.0.0
```
2. Run "kubectl get managedcertificate" command in the console to ensure that status is "Active":
```
NAME               AGE     STATUS
GCP Project       time     Active
```
3. Open Beam Playground frontend webpage in a web browser (e.g. https://playground.zone) to ensure that Playground frontend page is available
