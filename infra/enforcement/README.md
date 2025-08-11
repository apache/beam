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
The tool supports three different actions when discrepancies are found:

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

- **check**: Validates IAM policies and reports any differences (default behavior)
- **issue**: Creates a GitHub issue when IAM policies differ from the defined ones
- **generate**: Updates the compliance file to match the current GCP IAM policy

### Configuration

The `config.yml` file supports the following parameters:

- `project_id`: GCP project ID to check
- `users_file`: Path to the YAML file containing expected IAM policies
- `action`: Default action to perform (`check`, `issue`, or `generate`)
- `logging`: Logging configuration (level and format)

Command-line arguments take precedence over configuration file settings.
