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

## Creating GCP resources using Terraform

This directory provisions required infrastructure for the application.

# Requirements

See [playground/README.md](../README.md) for a list of the requirements
prior to following these instructions.

# Usage

```bash
$ terraform apply -target=module.infrastructure
```
## Terraform init

Follow conventional terraform workflow to build this solution.
You will be prompted for required variables.
Alternatively, you may create a `vars.tfvars` file and
apply the `-var-file=vars.tfvars` flag.

Initialize the terraform environment.

```

Type `yes` and hit **Enter**. Applying of the configuration could take several minutes. `Apply complete!` will be
displayed when it is finished, along with the number of created resources.
terraform init -target=module.infrastructure
```

## Terraform plan

To apply non-default Terraform configuration, pass the corresponding values as a variables with `terraform apply`
command. All variables are listed in the [variables.tf](variables.tf) file.
Plan the terraform solution.

```
terraform plan -target=module.infrastructure
```

or

* GCP [Artifact Registry](https://cloud.google.com/artifact-registry) to store application docker files
* [VPC](https://cloud.google.com/vpc) to run GCP [App Engine](https://cloud.google.com/appengine) VMs
* 2 GCP [Cloud Storage buckets]((https://cloud.google.com/storage/docs/key-terms#buckets)) to store Examples for
  Playground Application and Terraform states
* [GKE](https://cloud.google.com/kubernetes-engine) cluster for Playground's CI/CD process
```
terraform plan -var-file=vars.tfvars -target=module.infrastructure
```

## Terraform apply

Apply the terraform solution.

```

For more details on destroying deployed resources, please see
this [documentation](https://www.terraform.io/cli/commands/destroy).
terraform apply -target=module.infrastructure
```

or

```
terraform apply -var-file=vars.tfvars -target=module.infrastructure
```