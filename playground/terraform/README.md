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

## Requirements and setup

In order install a Playground cluster on GCP using Terraform, you'll need:

* An [GCP account](https://cloud.google.com/) and the [`gcloud`](https://cloud.google.com/sdk/gcloud) command-line tool
* [Terraform](https://www.terraform.io/downloads.html) tool

You'll also need to make sure that you're currently logged into your GCP account via the `gcloud` tool:

```bash
$ gcloud auth login
```
In other case you'll need an environment variable `GOOGLE_APPLICATION_CREDENTIALS` set to JSON key for service account that will be used to deploy resources

## Installation

You can install Terraform using the instructions [here](https://www.terraform.io/intro/getting-started/install.html).



## Creating GCP resources using Terraform

To get started building GCP resources with Terraform, you'll need to install all Terraform dependencies:

```bash
$ terraform init
# This will create a .terraform folder
```

Once you've done that, you can apply the default Terraform configuration:

```bash
$ terraform apply
```

You should then see this prompt:

```bash
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` and hit **Enter**. Applying the configuration could take several minutes. When it's finished, you should see `Apply complete!` along with some other information, including the number of resources created.

### Applying a non-default configuration

You can apply a non-default Terraform configuration by changing the values in the `terraform.tfvars` file. The following variables are available:

Variable name | Description | Default
:-------------|:------------|:-------
`project_id` | GCP Project ID that will be used to create resources | None
`docker_registry_address` | Address of docker registry | None
`docker_image_name` | Docker Image Name To Be Deployed | `beam_playground-backend`/`beam_playground-frontend`
`docker_image_tag` | Docker Image Tag To Be Deployed | `latest`
`repository_location` | Location of Artifact Registry | `us-central1`
`repository_id` | ID of Artifact Registry | `playground_repository`
`examples_bucket_name` | Name of Bucket to Store Playground Examples | `playground-examples`
`examples_bucket_location` | Location of Playground Examples Bucket | `US`
`examples_storage_class` | Examples Bucket Storage Class | `STANDARD`
`terraform_bucket_name` | Name of Bucket to Store Terraform States | `playground-terraform`
`terraform_bucket_location` | Location of Playground Terraform Bucket | `US`
`terraform_storage_class` | Terraform Bucket Storage Class | `STANDARD`
`vpc_name` | Name of VPC to be created | `playground-vpc`
`create_subnets` | Auto Create Subnets Inside VPC | `true`
`mtu` | MTU Inside VPC | `1460`

### What is installed

After applying terraform following resources will be created:

* GCP [Artifact Registry](https://cloud.google.com/artifact-registry) used to store application docker files:
* VPC to run GCP App Engine VM's ([VPC](https://cloud.google.com/vpc))
* 2 buckets to store Examples for Playground Application and Terraform states [buckets](https://cloud.google.com/storage/docs/key-terms#buckets)
* 2 GCP [App Engine](https://cloud.google.com/appengine) services for backend and frontend applications


### Destroying your cluster

At any point, you can destroy all GCP resources associated with your cluster using Terraform's `destroy` command:

```bash
$ terraform destroy
```


