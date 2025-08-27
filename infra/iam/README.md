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

This document outlines the structure of the Beam project control of infrastructure permissions and
 provides instructions on how to manage a user or role's permissions.

## Overview

### Managing User Roles

To manage user roles, edit the `users.yml` file. Add or modify entries under the `users` section to
 reflect the desired roles for each user. Remember to follow the YAML format:

```yaml
users:
  - username: <username>
    email: <email>
    permissions:
      - role: <role>
        title: <title> (optional)
        description: <description> (optional)
        expiry_date: <expiry_date> (optional, format: YYYY-MM-DD)
      - role: <role> (optional, for multiple roles)
```

> **Note**: `role/owner` roles are handled separately, adding them to the `users.yml` file will be ignored.

### Applying Changes

After modifying the `users.yml` file, open a Pull Request (PR) to the `infra/iam` directory.
The changes will be reviewed and when approved, they will be merged into the main branch.

Once the PR is merged, [the GitHub Actions workflow](../../.github/workflows/beam_UserRoles.yml)
 will automatically trigger and apply the changes to the IAM policies in the GCP project using Terraform.

This will update the IAM policies in the GCP project based on the changes made in the `users.yml` file.

## Directory Structure

### Terraform Configuration

- **main.tf**: The main Terraform configuration file that defines the infrastructure resources and their permissions.
- **config.auto.tfvars**: Contains the configuration variables for the Terraform project.
- **users.tf**: Processes the `users.yml` file to associate users with their respective roles.
- **users.yml**: A YAML file that contains the IAM policies and permissions for users and roles in the Beam project.

### Migration and Automation

- **migrate_roles.py**: Python script for migrating existing IAM policies to the new custom roles structure

## Custom Roles

The Beam project uses custom IAM roles to provide granular permissions for different levels of access to GCP resources. These roles follow a hierarchical structure where higher-level roles inherit permissions from lower-level roles.

### Role Hierarchy

The custom roles are structured in the following hierarchy:

```
beam_viewer < beam_writer < beam_infra_manager < beam_admin
```

### Available Roles

#### beam_viewer
- **Description**: Read-only access to the Beam project resources
- **Permissions**: View-only access to all services used by Beam
- **Exclusions**: Secret management permissions, destructive actions
- **Use case**: For team members who need to monitor and observe project resources

#### beam_writer
- **Description**: User access to resources in the Beam project
- **Permissions**: Inherits all `beam_viewer` permissions plus additional permissions for:
  - BigQuery data access and querying
  - Cloud SQL instance usage
  - Container cluster viewing and development
  - Datastore usage
  - Network viewing
- **Exclusions**: Destructive actions, administrative operations
- **Use case**: For active contributors who need to work with project resources

#### beam_infra_manager
- **Description**: Editor access to the Beam project infrastructure
- **Permissions**: Inherits all `beam_writer` permissions plus:
  - Cloud Build editor access
  - Service account token creation and usage
  - Storage object creation and viewing
  - General editor role (with exclusions)
- **Exclusions**: Destructive permissions, full administrative access
- **Use case**: For infrastructure maintainers who manage deployments and resources

#### beam_admin
- **Description**: Full administrative access to the Beam project
- **Permissions**: Complete access including:
  - All previous role permissions
  - Administrative access to all services
  - Secret management capabilities
  - Destructive operations
- **Exclusions**: None
- **Use case**: For project administrators and senior maintainers

### Managing Custom Roles

Custom roles are defined and managed through configuration files in the `roles/` directory:

- **roles_config.yaml**: Defines the roles, their hierarchy, services, and base permissions
- **generate_roles.py**: Python script that generates YAML role definitions from the configuration
- **roles.tf**: Terraform configuration that applies the custom roles to the GCP project

To modify custom roles:

1. Edit the `roles_config.yaml` file to update role definitions
2. Run `generate_roles.py` to regenerate the role YAML files
3. Apply changes through Terraform or via pull request

For detailed information about custom roles management, see the [roles directory README](roles/README.md).

### Migrating from Legacy Roles

The `migrate_roles.py` script helps migrate existing GCP project IAM policies to the new custom roles structure. This is useful when transitioning from standard GCP roles to the custom Beam roles.

#### Migration Rules

The script applies the following hierarchical migration rules:

- **Owner roles**: Left unchanged (highest privilege)
- **Admin/Secret roles**: Migrated to `beam_admin` (includes all lower roles)
- **Editor roles**: Migrated to `beam_infra_manager` (includes writer and viewer)
- **User roles**: Migrated to `beam_writer` (includes viewer)
- **Viewer roles**: Migrated to `beam_viewer`

#### Using the Migration Script

**Prerequisites:**
- Google Cloud SDK installed and authenticated
- Required Python dependencies (install with `pip install -r requirements.txt`)
- Appropriate GCP permissions to read IAM policies

**Export and migrate IAM policies:**
```bash
python migrate_roles.py <PROJECT_ID>
```

This generates two files:
- `<PROJECT_ID>.original-roles.yaml`: Current IAM policy export
- `<PROJECT_ID>.migrated-roles.yaml`: Proposed migration to custom roles

**Analyze permission differences for a specific user:**
```bash
python migrate_roles.py <PROJECT_ID> --difference <USER_EMAIL>
```

This generates:
- `<PROJECT_ID>.permission-differences.yaml`: Detailed comparison of permissions before and after migration

**Example workflow:**
```bash
# Export current IAM policies and generate migration
python migrate_roles.py apache-beam-testing

# Check permission differences for a specific user
python migrate_roles.py apache-beam-testing --difference user@example.com

# Review the generated files before applying changes
# Then apply via Terraform or manual IAM policy updates
```

The migration script helps ensure a smooth transition to the custom roles while maintaining appropriate access levels for all users.
