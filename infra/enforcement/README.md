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

# Infrastructure rules enforcement

This module is used to check that the infrastructure roles are being used.

## IAM Policies

The enforcement is done by validating the IAM policies against the defined policies.
The tool monitors and enforces compliance for user permissions, service account roles, and group memberships across your GCP project.

### Usage

You can specify the action either through the configuration file (`config.yml`) or via command-line arguments:

```bash
# Check compliance and report issues (default)
python iam.py --action check

# Create GitHub issue if compliance violations are found
python iam.py --action issue

# Generate new compliance file based on current IAM policy
python iam.py --action generate
```

### Actions

- **check**: Validates IAM policies against defined policies and reports any differences (default behavior)
- **issue**: Creates a GitHub issue when IAM policies differ from the defined ones, including detailed permission discrepancies
- **generate**: Updates the compliance file to match the current GCP IAM policy, creating a new baseline from existing permissions

### Features

The IAM Policy enforcement tool provides the following capabilities:

- **Comprehensive Policy Export**: Automatically exports all IAM bindings and roles from the GCP project
- **Member Type Recognition**: Handles users, service accounts, and groups with proper parsing and identification
- **Permission Comparison**: Detailed comparison between expected and actual permissions for each user
- **Conditional Role Filtering**: Automatically excludes conditional roles (roles with conditions) from compliance checks
- **Sorted Output**: Provides consistent, sorted output for easy comparison and review
- **Detailed Reporting**: Comprehensive reporting of permission differences with clear before/after comparisons
- **GitHub Integration**: Automatic issue creation with detailed compliance violation reports

### Configuration

The `config.yml` file supports the following parameters for IAM policies:

- `project_id`: GCP project ID to check (default: `apache-beam-testing`)
- `users_file`: Path to the YAML file containing expected IAM policies (default: `../iam/users.yml`)
- `action`: Default action to perform (`check`, `issue`, or `generate`)
- `logging`: Logging configuration (level and format)

### IAM Policy File Format

The IAM policy file should follow this YAML structure:

```yaml
- username: john.doe
  email: john.doe@example.com
  permissions:
    - role: roles/viewer
    - role: roles/storage.objectViewer
- username: service-account-name
  email: service-account-name@project-id.iam.gserviceaccount.com
  permissions:
    - role: roles/compute.instanceAdmin
    - role: roles/iam.serviceAccountUser
```

Each user entry includes:
- `username`: The derived username (typically the part before @ in email addresses)
- `email`: The full email address of the user or service account
- `permissions`: List of IAM roles assigned to this member
  - `role`: The full GCP IAM role name (e.g., `roles/viewer`, `roles/editor`)

### Compliance Checking Process

1. **Policy Extraction**: Retrieves current IAM policy from the GCP project
2. **Member Parsing**: Parses all IAM members and extracts usernames, emails, and types
3. **Role Processing**: Processes all roles while filtering out conditional bindings
4. **Comparison**: Compares current permissions with expected permissions from the policy file
5. **Reporting**: Generates detailed reports of any discrepancies found

Command-line arguments take precedence over configuration file settings.

## Account Keys

The enforcement is also done by validating service account keys and their access permissions against the defined policies.
The tool supports three different actions when discrepancies are found:

### Usage

You can specify the action either through the configuration file (`config.yml`) or via command-line arguments:

```bash
# Check compliance and report issues (default)
python account_keys.py --action check

# Create GitHub issue if compliance violations are found
python account_keys.py --action issue

# Generate new compliance file based on current service account keys policy
python account_keys.py --action generate
```

### Actions

- **check**: Validates service account keys and their permissions against defined policies and reports any differences (default behavior)
- **issue**: Creates a GitHub issue when service account keys policies differ from the defined ones
- **generate**: Updates the compliance file to match the current GCP service account keys and Secret Manager permissions

### Features

The Account Keys enforcement tool provides the following capabilities:

- **Service Account Discovery**: Automatically discovers all active (non-disabled) service accounts in the project
- **Secret Manager Integration**: Monitors secrets created by the beam-infra-secret-manager service
- **Permission Validation**: Ensures that Secret Manager permissions match the declared authorized users
- **Compliance Reporting**: Identifies missing service accounts, undeclared managed secrets, and permission mismatches
- **Automatic Remediation**: Can automatically update the compliance file to match current infrastructure state

### Configuration

The `config.yml` file supports the following parameters for account keys:

- `project_id`: GCP project ID to check
- `service_account_keys_file`: Path to the YAML file containing expected service account keys policies (default: `../keys/keys.yaml`)
- `action`: Default action to perform (`check`, `issue`, or `generate`)
- `logging`: Logging configuration (level and format)

### Service Account Keys File Format

The service account keys file should follow this YAML structure:

```yaml
service_accounts:
- account_id: example-service-account
  display_name: example-service-account@project-id.iam.gserviceaccount.com
  authorized_users:
  - email: user1@example.com
  - email: user2@example.com
```

Each service account entry includes:
- `account_id`: The unique identifier for the service account (without the full email domain)
- `display_name`: The full service account email address or any custom display name
- `authorized_users`: List of users who should have access to the service account's secrets
