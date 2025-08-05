# Service Account Management

This module is used to manage Google Cloud service accounts, including creating, retrieving, enabling, and deleting service accounts and their keys. It uses the Google Cloud IAM API to perform these operations.

## Features

- Create service accounts and service account keys.
- Store service account keys securely in Google Secret Manager.
- Rotate service account keys regularly.
- Enable and disable service accounts.

## Usage

We use the main.py script to manage service account keys. The script can be run with different commands to create, rotate, or retrieve service account keys.

### Prerequisites

- Google Cloud SDK installed and configured.
- Appropriate permissions to manage service accounts and secrets in your Google Cloud project.
- Required Python packages installed (see requirements.txt).

### Configuration

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

- `account_id`: The unique identifier for the service account.
- `display_name`: A human-readable name for the service account.
- `authorized_users`: A list of users who will be granted access to the service account.

The accounts defined in this file will be created if they do not already exist when running the script.

### Rotation

Service account keys should be rotated regularly to maintain security. The `--generate-key` flag in the `main.py` script can be used to create a new key for a service account. The script will also disable and delete the oldest key, ensuring that at least one key remains active.

```bash
python main.py --generate-key my-service-account
```

#### Cron rotation

To automate key rotation, you can set up a cron job that runs the command:

```bash
python main.py --cron
```

This will rotate keys for all service accounts defined in the `keys.yaml` file that have achieved the age threshold (e.g., 30 days). The age threshold can be adjusted in the `config.yaml` file.

### Retrieval

To retrieve the latest service account key, use the `--get-key` flag:

```bash
python main.py --get-key my-service-account
```

