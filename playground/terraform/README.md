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

# Requirements

The following items need to be setup for the Playground cluster deployment on GCP:

* [GCP account](https://cloud.google.com/)
* [`gcloud` command-line tool](https://cloud.google.com/sdk/gcloud) and required setup i.e. login
* [Terraform](https://www.terraform.io/downloads.html) tool
* [Docker](https://www.docker.com/get-started)

# Deployment steps

## GCP setup
1. Create a Service Account and download JSON key
2. Enable
   - Identity and Access Management (IAM) API
   - Cloud Storage API
   - Cloud Billing API
   - App Engine Admin API
3. Add a global `Owner` role to the Service Account in IAM console
4. Set environment vaiables for below steps:
```
export GOOGLE_APPLICATION_CREDENTIALS=<sa_credentials_file>
export PROJECT_ID=<project_id>
```
4. Login under SA account in gcloud
```
gcloud auth login --cred-file <sa_credentials_file>
gcloud auth activate-service-account --key-file <sa_credentials_file>
```

## 0. Create GCS bucket for state
**note** Bucket name is unique across all your projects, including those you've shut down

```bash
$ gsutil mb -p ${PROJECT_ID} gs://state-bucket-name
$ gsutil versioning set on gs://state-bucket-name
```

## 1. Create new environment

To provide information about the terraform backend, run the following commands

* New environment folder

```bash
mkdir /path/to/beam/playground/terraform/environment/<env-name>
```

* Backend config
Set storage bucket to keep remote state (created at step 0)

```bash
echo 'bucket = "state-bucket-name"' > /path/to/beam/playground/terraform/environment/{env-name}/state.tfbackend
```

* Terraform variables config and provide necessary variables

```bash
# copy default variables
cp playground/terraform/environment/beta/terraform.tfvars \
   playground/terraform/environment/{env-name}/terraform.tfvars
# set project-specific variables
echo -n '
project_id = "tour-of-beam"
state_bucket = "state-bucket-name"
bucket_examples_name = "state-bucket-name-examples"
' >> playground/terraform/environment/{env-name}/terraform.tfvars
```

## 2. Provision infrastructure

To deploy Playground infrastructure run gradle task:

```bash
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="{env-name}"
```

## 3. Deploy application

To deploy application run following steps:

* Authenticate to Artifact registry
We choose `us-central1` as a region here

```bash
$ gcloud auth configure-docker us-central1-docker.pkg.dev

$ base64 <sa_credentials_file> | docker login -u _json_key_base64 --password-stdin https://us-central1-docker.pkg.dev

```

* Deploy backend services
safe setting for `<tag>` is the same as `<env-name>`

```bash
./gradlew playground:terraform:deployBackend -Pproject_environment="<env-name>" -Pdocker-tag="<tag>"
```

* Deploy frontend service

```bash
./gradlew playground:terraform:deployFrontend -Pproject_environment="<env-name>" -Pdocker-tag="<tag>" ```

