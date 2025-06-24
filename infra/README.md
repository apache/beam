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

This will update the `beam_roles` directory with the new role definitions. To apply the changes to the GCP project, run the following Terraform commands:

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

---

## Python Scripts

This project includes a Python script to automate certain tasks.

### `export-users.py`

This script exports the roles that each user has in a GCP project. To run the script, execute the following command:

```bash
python3 export-users.py <project_id> <output_file>
```

It will generate a YAML file with the roles assigned to each user in the specified GCP project.