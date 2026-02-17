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

# GCP Security Analyzer

This document describes the implementation of a security analyzer for Google Cloud Platform (GCP) resources. The analyzer is designed to enhance security monitoring within our GCP environment by capturing critical events and generating alerts for specific security-sensitive actions.

## How It Works

1.  **Log Sinks**: The system uses [GCP Log Sinks](https://cloud.google.com/logging/docs/export/configure_export_v2) to capture specific security-related log entries. These sinks are configured to filter for events like IAM policy changes or service account key creation.
2.  **Log Storage**: The filtered logs are routed to a dedicated Google Cloud Storage (GCS) bucket for persistence and analysis.
3.  **Report Generation**: A scheduled job runs weekly, executing the `log_analyzer.py` script.
4.  **Email Alerts**: The script analyzes the logs from the past week, compiles a summary of security events, and sends a report to a configured email address.

## Configuration

The behavior of the log analyzer is controlled by a `config.yml` file. Here’s an overview of the configuration options:

-   `project_id`: The GCP project ID where the resources are located.
-   `bucket_name`: The name of the GCS bucket where logs will be stored.
-   `logging`: Configures the logging level and format for the script.
-   `sinks`: A list of log sinks to be created. Each sink has the following properties:
    -   `name`: A unique name for the sink.
    -   `description`: A brief description of what the sink monitors.
    -   `filter_methods`: A list of GCP API methods to include in the filter (e.g., `SetIamPolicy`).
    -   `excluded_principals`: A list of service accounts or user emails to exclude from monitoring, such as CI/CD service accounts.

### Example Configuration (`config.yml`)

```yaml
project_id: your-gcp-project-id
bucket_name: your-log-storage-bucket

sinks:
  - name: iam-policy-changes
    description: Monitors changes to IAM policies.
    filter_methods:
      - "SetIamPolicy"
    excluded_principals:
      - "ci-cd-account@your-project.iam.gserviceaccount.com"
```

## Usage

The `log_analyzer.py` script provides two main commands for managing the security analyzer.

### Initializing Sinks

To create or update the log sinks in GCP based on your `config.yml` file, run the following command:

```bash
python log_analyzer.py --config config.yml initialize
```

This command ensures that the log sinks are correctly configured to capture the desired security events.

### Generating Weekly Reports

To generate and send the weekly security report, run this command:

```bash
python log_analyzer.py --config config.yml generate-report
```

This is typically run as a scheduled job (GitHub Action) to automate the delivery of weekly security reports.



