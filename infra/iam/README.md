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

## Directory Structure


### Requirements before Running

- Change the `config.auto.tfvars` file to include the project ID you want to manage.
- Change the `main.tf` file to include the correct google cloud bucket where the terraform state will be stored.

### Terraform Configuration

- **main.tf**: The main Terraform configuration file that defines the infrastructure resources and their permissions.
- **config.auto.tfvars**: Contains the configuration variables for the Terraform project.
- **users.tf**: Processes the `users.yml` file to associate users with their respective roles.
- **users.yml**: A YAML file that contains the IAM policies and permissions for users and roles in the Beam project.
  This file is formatted as follows:

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

### Python Scripts

- **generate.py**: A script that exports the IAM policy for a specified GCP project and saves it in YAML format. It can be used to generate the `users.yml` file based on the current IAM policy. This script does not support exporting conditional roles.

## Usage Instructions

### Exporting IAM Policy

To export the IAM policy for a specific GCP project, you can use the `generate.py` script. This script requires the `google-cloud-resourcemanager` library to be installed. You can install this and other required libraries using pip:

```bash
pip install -r requirements.txt
```

Then, you can run the script with the following command:

```bash
python generate.py apache-beam-testing users.yml
```

This command will export the IAM policy for the project `apache-beam-testing` and save it to `users.yml`. If you want to specify a different output file, you can do so by providing the desired filename as an argument:

```bash
python generate.py apache-beam-testing custom_output.yml
```

### Managing User Permissions

To manage user permissions, you can edit the `users.yml` file directly. Add or modify entries under the `users` section to reflect the desired permissions for each user. Remember to follow the YAML format specified above.

### Applying Changes

After making changes to the `users.yml` file, you can apply the changes using Terraform. First, ensure that you have initialized Terraform in your project directory:

```bash
terraform init
```

Then, you can apply the changes with the following commands:

```bash
terraform plan
terraform apply
```

This will update the IAM policies in your GCP project based on the changes made in the `users.yml` file.

