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

## Deployment to AppEngine

*Note: All requirements are listed in the [README.md](../README.md) of the Terraform module.*

Installation of all Terraform dependencies is required to get started building GCP resources with Terraform:

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

Type `yes` and hit **Enter**. Applying of the configuration could take several minutes. `Apply complete!` will be
displayed when it is finished, along with the number of created resources.

### Applying a non-default configuration

To apply non-default Terraform configuration, pass the corresponding values as a variables with `terraform apply`
command. All variables are listed in the [variables.tf](variables.tf) file.

Each of service will be deployed with default resources and scaling configuration. It might be changed in following
files:

* [Go](./backend/backend-go/main.tf)
* [Java](./backend/backend-java/main.tf)
* [Python](./backend/backend-python/main.tf)
* [SCIO](./backend/backend-scio/main.tf)
* [Router](./backend/backend-router/main.tf)

### Destroy deployed resources

At any point, all GCP resources created by terraform can be destroyed using the following command:

```bash
$ terraform destroy
```

For more details on destroying deployed resources, please see
this [documentation](https://www.terraform.io/cli/commands/destroy).