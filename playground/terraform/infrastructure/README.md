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

Installation of all Terraform dependencies required to get started building GCP resources with Terraform:

```bash
$ terraform init
# This will create a .terraform folder
```

Once it has been done, default Terraform configuration can be applied:

```bash
$ terraform apply
```

Then the following dialog will be displayed in the console:

```bash
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` and hit **Enter**. Applying the configuration could take several minutes. `Apply complete!` will be displayed
when it's finished, along with some other information, including the number of resources created.

### Applying a non-default configuration

To apply non-default Terraform configuration, change the [variables.tf](variables.tf) with custom values.

### What is installed

After applying terraform following resources will be created:

* GCP [Artifact Registry](https://cloud.google.com/artifact-registry) used to store application docker files:
* VPC to run GCP App Engine VM's ([VPC](https://cloud.google.com/vpc))
* 2 buckets to store Examples for Playground Application and Terraform
  states [buckets](https://cloud.google.com/storage/docs/key-terms#buckets)
* GKE cluster for Playground's CI/CD process.

### Destroy deployed resources

At any point, all GCP resources created by terraform can be destroyed using `terraform destroy` command:

```bash
$ terraform destroy
```

For more details please see this [documentation](https://www.terraform.io/cli/commands/destroy)

