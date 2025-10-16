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

# Service Account Management

This module is used to manage Google Cloud service accounts, including creating, retrieving, enabling, and deleting service accounts and their keys. It uses the Google Cloud IAM API to perform these operations.

## User usage

We use the `keys.py` script to manage service account keys. In order to use this script you will need to:

1. Generate a change over `keys.yaml` file, where your email address is listed as an authorized user for the service accounts you want to manage.
2. Open a pull request with the changes to the `keys.yaml` file. The change will be reviewed and merged by the Infra team.
3. Once your changes are merged, install the required python packages: `pip install -r requirements.txt` and authenticate with Google Cloud using the `gcloud auth application-default login` command.
4. Run `keys.py --get-key <service_account_id>` to get the latest key for a service account. The key will be printed to the console, and you can use it to authenticate with Google Cloud services.

> Remember this keys are rotated regularly, so you will need to run the command again to get the latest key after a rotation, the rotation days are defined in the `config.yaml` file.

## Administrative usage

This section is intended for developers who need to manage service accounts and their keys at a higher level. A regular user should not need to access this section.

### Prerequisites

- Google Cloud SDK installed and configured.
- Appropriate permissions to manage service accounts and secrets in your Google Cloud project.
- Required Python packages installed (see requirements.txt).

### How it works

This module provide a script `keys.py` that allows you to manage the service accounts and their keys. This script is run automatically by a GitHub Action to ensure that service account keys are rotated regularly and that the latest keys are available for authorized users. It is also run every time a PR is merged over the `keys.yaml` file to ensure that the service accounts, their keys and authorized users are up to date.

### Automation with GitHub Actions

A GitHub Actions workflow is set up to automate the execution of the `keys.py` script. This workflow is defined in `.github/workflows/beam_Infrastructure_ServiceAccountKeys.yml`.

The workflow is triggered automatically on the following events:
- A push to the `main` branch that includes changes to the `infra/keys/keys.yaml` file.
- A manual trigger (`workflow_dispatch`) by a developer.

When triggered, the workflow runs the `python keys.py --cron` command, which handles the creation and rotation of service account keys based on the configuration in `keys.yaml` and `config.yaml`.

### Files

#### config.yaml

This file contains configuration settings for the service account management, including project ID, key rotation settings, and logging configuration.

#### keys.yaml

All the service accounts are managed through a configuration file in YAML format, `keys.yaml`. This file contains the necessary information about each service account, including its ID, display name, and authorized users.

```yaml
service_accounts:
  - account_id: my-service-account
    display_name: My Service Account
    authorized_users:
      - email: user1@example.com
      - email: user2@example.com
```

Where:

- `account_id`: The unique identifier for the service account. The email address of the service account will be `<account_id>@<project_id>.iam.gserviceaccount.com`.
- `display_name`: A human-readable name for the service account.
- `authorized_users`: A list of users who will be granted access to the service account's keys. Each user is specified by their email address. This users will be able to retrieve the keys and act on behalf of the service account.

Service accounts are created the first time the cron is run, or when the `keys.yaml` file is updated with a new service account. The script checks if the service account already exists in Google Cloud, and if not, it creates it. If the service account already exists but it is not managed by the Secret Manager, it creates a new key, storing it in the Secret Manager and ignore the rest. This ensures that the service account is always up to date with the latest keys and authorized users.

### Rotation

Service account keys should be rotated regularly to maintain security. To automate key rotation, you can set up a cron job that runs the command:

```bash
python keys.py --cron
```

This will rotate keys for all service accounts defined in the `keys.yaml` file that have achieved the age threshold (e.g., 30 days). The age threshold can be adjusted in the `config.yaml` file.

### Retrieval

To retrieve the latest service account key, use the `--get-key` flag:

```bash
python keys.py --get-key my-service-account
```

