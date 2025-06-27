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

# Infrastructure Permissions Management

This document outlines the structure of the Beam project control of infrastructure permissions and provides instructions on how to manage a user or role's permissions.

## Project Overview

This project uses Terraform to manage Google Cloud Platform (GCP) resources. The main components are:

- `main.tf`: The main Terraform file that defines the GCP provider and project-level configurations.
- `users.tf`: Manages user permissions by reading from the `users.yml` file.
- `users.yml`: A YAML file that lists all users and their assigned roles.
- `beam_roles/`: A Terraform module that defines custom IAM roles for the project.
- `export-roles.py`: A Python script to export the roles defined on a GCP project.

---

## Roles

Roles are defined in the `beam_roles` directory. The component files include:

- `roles_config.yaml`: A YAML file that defines the roles and their associated services.
- `generate_roles.py`: A Python script that generates Terraform files based on the roles defined in `roles_config.yaml`.
- `roles.tf`: A Terraform file that applies the generated roles to the GCP project.

### Defined roles

The roles are defined in the `roles_config.yaml` file. Each role includes a name, description, and a list of services associated with it.

The defined roles are:

- `beam_viewer`: Read-only access to the Beam project. Excludes secret management permissions.
- `beam_committer`: Similar to `beam_viewer`, but for users actively contributing code.
- `beam_infra_manager`: Editor access to the Beam project, excluding destructive permissions.
- `beam_admin`: Full access to the Beam project, including destructive capabilities and secret management.

Roles are structured in a hierarchy, allowing for inheritance of permissions. Each role builds upon the previous one. The hierarchy is as follows:

```plaintext
beam_viewer < beam_committer < beam_infra_manager < beam_admin
```

### Modifying Roles services

Each role can have its associated services modified. The services are defined in the `roles_config.yaml` file under each role's `services` section.

To modify the services associated with a role, edit the `roles_config.yaml` file and update the relevant service lists under each role. After making changes, re-run the `generate_roles.py` script to apply the updates.

The `generate_roles.py` script, install the dependencies using:

```bash
pip install -r requirements.txt
```

After modifying the `roles_config.yaml` file, run the script to generate the yaml files for the roles:

```bash
python3 generate_roles.py
```

This will update the `beam_roles` directory with the new role definitions. You do not need any GCP permissions to run this script, as it only generates local files.

To apply the changes to the GCP project, ensure you have a owner role in the GCP project and run the following Terraform commands:

```bash
terraform plan
terraform apply
```

---

## Users

User permissions are managed through the `users.yml` file. Each user is defined with their email and a list of assigned roles.

### Adding a New User

To add a new user, add a new entry to the `users.yml` file with the following format:

```yaml
- username: <username>
  email: <email>
  permissions:
    - role: <role>
      title: <title> (optional)
      description: <description> (optional)
      expiry_date: <expiry_date> (optional, format: YYYY-MM-DD)
```

Remember that roles have a hierarchy, so the assigned role should also include all permissions from lower roles. For example, if you assign `beam_infra_manager`, the user will also inherit permissions from `beam_viewer` and `beam_committer`.

After adding the user, run the following Terraform commands to apply the changes:

```bash
terraform plan
terraform apply
```

Remember that you need to have the necessary permissions in the GCP project to manage users and roles.

---

## Python Scripts

This project includes a Python script to automate certain tasks.

### `export-users.py`

This script exports the roles that each user has in a GCP project. To run the script, execute the following command:

```bash
python3 export-users.py <project_id> <output_file>
```

It will generate a YAML file with the roles assigned to each user in the specified GCP project.

## What to do when approved

This is a work in progress so some manual interaction is needed for the migration. First of all we need a GCP bucket to store the terraform state files. And change the `main.tf` file to point to that bucket. The bucket should be created in the same GCP project where you want to manage the permissions.

Then we need to modify the `config.auto.tfvars` file to include the GCP project ID we are working with. This file is used to set the variables for the Terraform configuration.

The idea is to run the `export-roles.py` script to export the roles defined on a GCP project. This will help in keeping track of the roles and their permissions. This would be the initial users file, running it with Terraform will tell terraform to keep track of the users and their permissions.

```bash
python3 export-roles.py <project_id> users.yml
terraform init
terraform plan
terraform apply
```

This will initialize Terraform, plan the changes for creating the custom roles, and apply the changes to the GCP project.

The `users.yml` file that exists in the repository was created based on the permissions found in the GCP project, migrating to the new custom roles and permissions. It can be seen that viewer was changed to beam_viewer and editor to beam_infra_manager, for users that had higher roles, they were assigned the beam_admin role. The owners where left as is. Something to keep in mind is that users roles are inherited, so if a user has `beam_infra_manager`, they will also have the `beam_viewer` role.

If approved, it is a matter of making it the new `users.yml` file and running the Terraform commands to apply the changes.

```bash
terraform plan # This will show the changes that will be applied
terraform apply # This will apply the changes to the GCP project
```

The idea would be that after the migration is done, a github action will be created to run the `generate_roles.py` script when a change is made to the `roles_config.yaml` file, and the terraform commands will be run to apply the changes to the GCP project. This way, the roles and permissions will be kept up to date with the changes made in the `roles_config.yaml` file.

About new users, they would add their email to the `users.yml` file with the desired role, this would be done on a pull request. When the pull request is merged, the GitHub action will run the `generate_roles.py` script and the Terraform commands to apply the changes to the GCP project. This way, new users can be added easily and their permissions will be managed through the `users.yml` file.
