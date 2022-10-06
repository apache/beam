<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# GitHub Actions - Self-hosted Linux Runners
These folders contain the required resources to deploy the GitHub Actions self-hosted runners for the workflows running in Ubuntu OS.
* /docker
    * Dockerfile and entrypoint: Resources needed to create a new self-hosted-runner image.
    * docker-compose.yml: In case you would like to test and run the self-hosted runner locally.

* /kubernetes
    * Kubernetes files to create the resources needed to deploy the self-hosted runners.

## Docker

#### How to build a new image and push it to the Artifact Registry?
* Create the image locally

`docker build -t $LOCAL_IMAGE_NAME:TAG .`

* Tag the local image with the GCP repository name

`docker tag $LOCAL_IMAGE_NAME LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE:latest`

* Make sure you are authenticated to the gcloud repository

`gcloud auth configure-docker us-central1-docker.pkg.dev`

* Push the tagged image to the repository

`docker push LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE:latest`

* _**Important: Please add the commit hash as a tag when a new image is uploaded.**_

#### How to run a self-hosted runner locally?

* Create a `.var.env` file from the `example.var.env` file and replace the corresponding values:
    * GOOGLE_APPLICATION_CREDENTIALS: Path where the json file will be stored (Same path as the one given in the docker-compose.yml file)
    * CLOUD_FUNCTION_NAME: Function where the self-hosted runner will get the RUNNER_TOKEN
    * ORG_RUNNER_GROUP: Name of the GitHub Actions Runner Group. You can find it here: https://github.com/organizations/ORG_NAME/settings/actions/runner-groups
    * ORG_NAME: Name of the Organization
    * GCP_REGION: GCP Region where your Cloud Function is deployed
    * GCP_PROJECT_ID: GCP Project ID where your Cloud Function is deployed


* Run the container

`docker compose up`

* You will be able to see the self-hosted runner in the Settings of your GitHub repository

## Kubernetes

For the current implementation we are using Google Kubernetes Engine (GKE) as a container orchestration platform.

* Configure your Kubernetes local context

`gcloud container clusters get-credentials $GCP_CLUSTER_NAME --zone $GCP_REGION --project $GCP_PROJECT-ID`

* Create the desired k8s namespace

`kubectl create namespace $NAMESPACE`

* Create the GKE secret from a json file

`kubectl create secret generic $k8s_SECRET_NAME --from-file=key.json=$LOCAL_PATH --namespace $NAMESPACE`

* Update the `github-actions-secrets.yml` file with its corresponding values encrypted in base64

`echo -n "$VARIABLE_VALUE" | base64`

* Replace in `github-actions-deployment.yml` file the `$IMAGE_URL` variable with the corresponding image URL: `GCP_LOCATION-docker.pkg.dev/GCP_PROJECT_ID/REPOSITORY_NAME/IMAGE_NAME`


* In case you would like to create the deployment from scratch, run the `run-k8s-deployment.sh` script to execute the Kubernetes deployment in the GKE cluster.
    * **Important: Make sure you have the GKE context selected in your local machine:** `kubectl config current-context`

`./run-k8s-deployment.sh $NAMESPACE`

* Otherwise, apply only the changes. **Important: Make sure you have the GKE context selected in your local machine:** `kubectl config current-context`

`kubectl apply -f github-actions-$FILE_NAME.yml --namespace $NAMESPACE`

* In case you would like to delete all the Kubernetes resources, run the `delete-k8s-deployment.sh` script with its corresponding namespace value.

`./delete-k8s-deployment.sh $NAMESPACE`