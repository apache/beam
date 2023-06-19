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
Ensure that the account has at least the following  [IAM roles](https://cloud.google.com/iam/docs/understanding-roles):
   - App Engine Admin
   - App Engine Creator
   - Artifact Registry Administrator
   - Cloud Datastore Index Admin
   - Cloud Datastore User
   - Cloud Memorystore Redis Admin
   - Cloud Functions Developer
   - Compute Admin
   - Create Service Accounts
   - DNS Administrator
   - Kubernetes Engine Admin
   - Quota Administrator
   - Role Administrator
   - Security Admin
   - Service Account User
   - Storage Admin

3. [Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) for Terraform state file

4. DNS name for your Playground deployment instance further referred as <playground.zone>

5. OS with installed software listed below:

* [Java](https://adoptopenjdk.net/)
* [Kubernetes Command Line Interface](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/)
* [HELM](https://helm.sh/docs/intro/install/)
* [Docker](https://docs.docker.com/engine/install/)
* [Terraform](https://www.terraform.io/downloads)
* [gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
* [Kubectl authentication](https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke)
* [GO](https://go.dev/doc/install)

6. Apache Beam Git repository cloned locally

# Prepare deployment configuration:
Playground uses `terraform.tfvars` located in `playground/terraform/environment/<environment_name>` to define variables specific to an environment (e.g., prod, test, staging).<br>
1. Create a folder (further referred as <environment_name>) to define a new environment and place configuration files into it:

* `terraform.tfvars` environment variables:
```
project_id             = "project_id"               # GCP Project ID
network_name           = "playground-network"       # GCP VPC Network Name for Playground deployment
subnetwork_name        = "playground-subnetwork"    # GCP VPC Subnetwork Name for Playground deployment
gke_name               = "playground-backend"       # Playground GKE Cluster name
region                 = "us-east1"                 # Deployment region for all created resources
zone                   = "us-east1-b"               # Deployment zone in the specified region
state_bucket           = "playground-state-bucket"  # GCS bucket name for Terraform state file. The bucket must be created manually before deployment
redis_name             = "playground-redis"         # Name for Redis instance
redis_tier             = "BASIC"                    # Redis tier type. Options: "Basic" or "Standard_HA". Default: "BASIC"
min_count              = 2                          # Min node count for the GKE cluster
max_count              = 6                          # Max node count for the GKE cluster
skip_appengine_deploy  = false                      # AppEngine flag - defined if AppEngine and Datastore need to be installed. Should be "true" if AppEngine and Datastore were installed before
ip_address_name        = "playground-static-ip"     # GCP Static IP Address name
repository_id          = "playground-artifacts"     # GCP Artifact repository name for Playground images
service_account_id     = "playground-gke-sa"   # GCP Service account name
gke_machine_type       = "e2-standard-8"            # Machine type for GKE Nodes
env                    = "prod"                     # Environment. The same value as for <environment_name> parameter

```
* `state.tfbackend` environment variables:
```
bucket               = "playground-state-bucket"         # GCS bucket name for Terraform state file. The same value as in terraform.tfvars file.
```
2. Configure authentication for the Google Cloud Platform
```
gcloud init
```
```
gcloud auth application-default login
```
# Deploy Playground infrastructure:
If you intend to install more than one environment, you may need to manually remove 'playground/terraform/.terraform' folder before a new attempt

1. Start the following command from the top level repository folder ("beam") to deploy the Payground infrastructure:
```
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="<environment_name>"
```

2. Find a Static IP in your GCP project>VPC Network>IP Addresses>pg-static-ip
<br>Add following DNS A records for the discovered static IP address:
```
java.<playground.zone>
python.<playground.zone>
scio.<playground.zone>
go.<playground.zone>
router.<playground.zone>
<playground.zone>
```
[More about DNS zone registration](https://domains.google/get-started/domain-search/)<br>
[More about A records in DNS](https://support.google.com/a/answer/2579934?hl=en)

# Deploy Playground to Kubernetes:

1. Run the following command to authenticate in the Docker registry:
```
gcloud auth configure-docker <region>-docker.pkg.dev
```
2. Run the following command to authenticate in GKE:
```
gcloud container clusters get-credentials --region <zone> <gke_name> --project <project_id>
```
Start the following command from the top level repository folder ("beam") to deploy the Payground infrastructure:
```
./gradlew playground:terraform:gkebackend -Pproject_environment="<environment_name>" -Pdocker-tag="tag" \
  -Pdns-name="<playground.zone>" -Psdk-tag=2.44.0 \
   -Pdocker-repository-root="<region>-docker.pkg.dev/<project_id>/<repository_id>" -Pdatastore-namespace="Playground"
```
Where tag - image tag for docker images, Psdk-tag - current Apache Beam SDK version, Pdatastore-namespace - namespace for Datastore

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

# Use HELM to update environment
Helm is responsible for deploying Beam Playground on the GKE cluster
Use the steps below to update HELM Chart and apply it:

1. Configure authentication for the Google Cloud Platform:
```
gcloud init
```
```
gcloud auth application-default login
```
2. Run the following command to authenticate in the Docker registry:
```
gcloud auth configure-docker <region>-docker.pkg.dev
```
3. Run the following command to authenticate in GKE:
```
gcloud container clusters get-credentials --region <zone> <gke_name> --project <project_id>
```
4. Clone Apache BEAM repository from Git
5. Make required changes in playground/infrastructure/helm-playground/values.yaml file:
```
replicaCount: 1
image:
   java_image: beam_playground-backend-java
   go_image: beam_playground-backend-go
   router_image: beam_playground-backend-router
   scio_image: beam_playground-backend-scio
   python_image: beam_playground-backend-python
   frontend_image: beam_playground-frontend
   pullPolicy: Always
service:
   type: NodePort
   targetPort: 8080
   port: 443
healthcheck:
   port: 8080
   livInitialDelaySeconds: 30
   livPeriodSeconds: 30
   readInitialDelaySeconds: 30
   readPeriodSeconds: 30
autoscaling:
   runners:
 	  maxReplicas: 4
 	  minReplicas: 2
   rest:
 	  maxReplicas: 4
 	  minReplicas: 1
   utilization:
 	memoryUtilization: 80
 	cpuUtilization: 95
static_ip: <external IP address>
redis_ip: <REDIS IP address>:6379
project_id: <Project ID>
registry: <region>-docker.pkg.dev/<project_id>/<repository_id>
static_ip_name: <ip_address_name>
tag: <docker-tag>
datastore_name: <datastore-namespace>
dns_name: <dns-name>
func_clean: https://<region>-<project_id>.cloudfunctions.net/playground-function-cleanup-<env>
func_put: https://<region>-<project_id>.cloudfunctions.net/playground-function-put-<env>
func_view: https://<region>-<project_id>.cloudfunctions.net/playground-function-view-<env>
```
6. Execute following command to update HELM:
```
helm upgrade playground /playground/infrastructure/helm-helm-playground
```
## Variables description for "values.yaml"
See Also:

[Services](https://cloud.google.com/kubernetes-engine/docs/concepts/service)

[Configure Liveness, Readiness and Startup Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)

[Cluster autoscaling](https://cloud.google.com/kubernetes-engine/docs/concepts/cluster-autoscaler#:~:text=GKE's%20cluster%20autoscaler%20automatically%20resizes,minimum%20size%20that%20you%20designate.)


|Variable                           |Description                                        |
|-----------------------------------|:--------------------------------------------------|
|service/type                       |connection type for service in the GKE Cluster     |
|service/targetPort                 |Service target port, connection from Service to POD|
|service/port                       |Service port, connection from ingress to service   |
|healthcheck/port                   |Internal POD port for Healthcheck                  |
|healthcheck/livInitialDelaySeconds |pre-polling delay                                  |
|healthcheck/livPeriodSeconds       |Poll period                                        |
|healthcheck/readInitialDelaySeconds|Check if POD is ready                              |
|healthcheck/readPeriodSeconds      |Poll period for checking if POD is ready           |
|autoscaling/runners/maxReplicas    |maximum number of PODs for a runner                |
|autoscaling/runners/minReplicas    |minimum number of PODs for a runner                |
|autoscaling/rest/maxReplicas       |max number of PODs per computer                    |
|autoscaling/rest/minReplicas       |minimum number of PODs for a router                |
|autoscaling/memoryUtilization      |POD scaling activation threshold based on RAM usage|
|autoscaling/cpuUtilization         |POD scaling activation threshold based on CPU usage|
