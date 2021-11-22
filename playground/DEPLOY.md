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
## Manual App Install
To install Playground applications into existed GCP account you'll need to follow several steps, in this document just simple set of commands will be provided that will guide you over install process.

Before starting that process, you will need to create GCP Artifact Registry instance to store there docker files that would be deployed. Guide to install that registry could be found in [that](https://github.com/apache/beam/blob/master/playground/terraform/README.md) document.
### Common steps
First of all you will need to get service account key in json format, that can be done by following [that](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) document. Let's assume that you saved that key into file named key.json.
Next step is to authentificate your account in GCP docker registry, this can be done by running command:
```bash
$ cat /root/dt-key.json | docker login -u _json_key --password-stdin REGISTRY_NAME
```
you will need to replace `REGISTRY_NAME` to actual registry address that can be found in Google Cloud console of via gcloud tool.

Next step is to specify path to json key to be used to deploy applications to Google App Engine. This could be done by command
```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=PATH_TO_KEY_JSON_FILE
```
replace `PATH_TO_KEY_JSON_FILE` with actual path to key.json file

### Deploy backend application

Now you can start deploying Playground backend application.
First of all you will need to build and push image to docker registry, this can be done by command
```bash
$ ./gradlew playground:backend:containers:java:dockerTagsPush -Pdocker-repository-root='REGISTRY_NAME' -Pbase-image='apache/beam_java8_sdk:BEAM_VERSION' -Pdocker-tag="BACKEND_TAG"
```
replace placeholders by
* `REGISTRY_NAME` with actual registry address, same as was defined in first command(docker login)
* `BEAM_VERSION` with version of Apache Beam that you want to use
* `BACKEND_TAG` tag that playground backend docker image will be set and pushed to repo

And final step for deploying backend is deploy via terraform, to run that you will need to run commands

```bash
$ cd playground/terraform/applications/backend/
$ terraform init
$ terraform apply -auto-approve -var="project_id=GCP_PROJECT_ID" -var="docker_registry_address=REGISTRY_NAME" -var="docker_image_tag=BACKEND_TAG" -var="service_name=BACKEND_SERVICE_NAME"
```

replace placeholdes by
* `GCP_PROJECT_ID` with ID for GCP project where you are deploying application
* `REGISTRY_NAME` with actual registry address, same as was defined in first command(docker login)
* `BACKEND_TAG` tag that playground backend docker image will be set and pushed to repo
* `BACKEND_SERIVICE_NAME` this should be service name for deployed application, could be something like backend-stage

Now you could go to GCP Console or run command `gcloud app services list` to verify that service with name that you specified was deployed successfully.
For next step you also will need to run command `gcloud app services browse BACKEND_SERVICE_NAME` to get URL of backend service, that will be needed to deploy frontend application.

### Deploy Frontend Application

Now you can start deploying Playground frontend application.
First of all you will need to prepare config file to build frontend application, this can be done by command
```bash
./gradlew playground:frontend:createConfig -PplaygroundBackendUrl="BACKEND_SERVICE_URL"
```
replace `BACKEND_SERVICE_URL` with url that you recieved on previous step(gcloud app services browse)

Next step is to build and push to registry docker image for frontend application, that can be done by running command
```bash
./gradlew --debug playground:frontend:dockerTagsPush -Pdocker-repository-root='REGISTRY_NAME'  -Pdocker-tag="FRONTEND_TAG"
```
replace placeholders like
* `REGISTRY_NAME` with actual registry address, same as was defined in first command(docker login)
* `FRONTEND_TAG` tag that playground frontend docker image will be set and pushed to repo

And finally deploy application to App Engine
```bash
$ cd playground/terraform/applications/frontend/
$ terraform init
$ terraform apply -auto-approve -var="project_id=GCP_PROJECT_ID" -var="docker_registry_address=REGISTRY_NAME" -var="docker_image_tag=FRONTEND_TAG" -var="service_name=FRONTEND_SERVICE_NAME"
```
replace placeholders by
* `GCP_PROJECT_ID` with ID for GCP project where you are deploying application
* `REGISTRY_NAME` with actual registry address, same as was defined in first command(docker login)
* `FRONTEND_TAG` tag that playground frontend docker image will be set and pushed to repo
* `FRONTEND_SERIVICE_NAME` this should be service name for deployed application, could be something like frontend-stage

Same as for backend application you can run gcloud commands to get url and list services or go to GCP Console and get url from there.
