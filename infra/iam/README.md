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
