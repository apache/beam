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
  * Kubernetes files to create the resources needed to deploy the sel-hosted runners.

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

#### How to run a self-hosted locally?

* Create a `.var.env` file from the `example.var.env` file and replace the corresponding values

* Run the container

`docker-compose up`

* You will be able to see the self-hosted runner in the Settings of your GitHub repository

## Kubernetes 
* Configure your Kubernetes local context

`gcloud container clusters get-credentials $CLUSTER_NAME --zone us-central1-a --project $PROJECT-ID`

* Update the `github-actions-secrets.yml` file with its corresponding encrypted in base64 values

`echo $GITHUB_TOKEN | base64`
`echo GITHUB_REPO | base64`

* Replace in `github-actions-deployment.yml` file the `$IMAGE_URL` variable with the corresponding image URL: `LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY/IMAGE`

* In case you would like to create the deployment from scratch, run the `run-k8s-deployment.sh` script to do the Kubernetes deployment in the GKE cluster. Make sure you have the GKE context selected in your local machine

`./run-k8s-deployment.sh`

* Otherwise, apply only the changes. Make sure you have the GKE context selected in your local machine

`kubectl apply -f github-actions-$FILE_NAME.yml`
