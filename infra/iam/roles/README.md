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

# Beam custom roles

This document describes the custom roles defined for the Beam project and their associated permissions.


## Roles

The following files are used to define and manage roles:

- `roles_config.yaml`: A YAML file that defines the roles and their associated services.
- `generate_roles.py`: A Python script that generates yaml files for the roles.
- `roles.tf`: A Terraform file that applies that generate the roles described over the custom roles created.

### Defined roles

The roles are defined in the `roles_config.yaml` file. Each role includes a name, description, and a list of services associated with it.

The defined roles are:

- `beam_viewer`: Read-only access to the Beam project. Excludes secret management permissions.
- `beam_writer`: User access to the the resources in the Beam project.
- `beam_infra_manager`: Editor access to the Beam project, excluding destructive permissions.
- `beam_admin`: Full access to the Beam project, including destructive capabilities and secret management.

Roles are structured in a hierarchy, allowing for inheritance of permissions. Each role builds upon the previous one. The hierarchy is as follows:

```plaintext
beam_viewer < beam_writer < beam_infra_manager < beam_admin
```

### Modifying Roles services

Each role can have its associated base roles and services. The `roles_config.yaml` file defines the services associated with each role. For example, the `beam_viewer` role has read-only access to the project, while the `beam_infra_manager` role has editor access but excludes destructive permissions.

To modify the services associated with a role, edit the `roles_config.yaml` file and update the relevant service and roles lists under each role. After making changes, re-run the `generate_roles.py` script to apply the updates.

The `generate_roles.py` script, install the dependencies using:

```bash
pip install -r requirements.txt
```

After modifying the `roles_config.yaml` file, run the script to generate the yaml files for the roles:

```bash
python3 generate_roles.py
```

This will update the `beam_roles` directory with the new role definitions. You do not need any GCP permissions to run this script, as it only generates local files.

To apply the changes to the GCP project, ensure you have a owner role in the GCP project, go to the main `infra/iam` directory and run the following Terraform commands:

```bash
terraform plan
terraform apply
```
