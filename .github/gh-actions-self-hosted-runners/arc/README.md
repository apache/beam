<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# Actions Runner Contoler

# About
Check out the docs at https://github.com/actions/actions-runner-controller/blob/master/docs/about-arc.md

# Installing
1. Create a bucket for terraform state, making sure you disable public access and allow your account to access it. (or reuse existing that is noted in the environment file)

2. Create a Github App in your account and install it in the repo you want to provide runners for.
All is explained in : https://github.com/actions/actions-runner-controller/blob/master/docs/authenticating-to-the-github-api.md

3. In your Google Cloud Project create the secrets for
- Github App ID
- Github App Installation ID
- Github App PEM key
All are created in the step before

4. Create a file called `environment_name.env` in the folder `environments` with the following contents:
```
project_id = "PROJECT_ID"                                     # google PROJECT_ID that you want to deploy in
region = "gcp_region"                                         # GCP region for the network
zone = "europe-west3-c"                                       # GCP zone for the nodes
min_main_node_count = "1"                                     # Minimal and initial node count for main pool
max_main_node_count = "5"                                     # Maximal node count for main pool
environment = "environment_name"                              # Name of the environment. Used as a prefix like dev- stag- anything-
ingress_domain = "fqdn"                                       # FQDN for webhook ingress
organization = "org"                                          # Github Organization to use runners in
repository = "repo"                                           # Repository to use runners in
github_app_id_secret_name = "app_id_secret_name"              # Google secret name for app id
github_app_install_id_secret_name = "install_id_secret_name"  # Google secret name for install_id
github_private_key_secret_name = "pem_file_secret_name"       # Google secret name for pem file
deploy_webhook = "false"                                      # Terraform to deploy the scaling webhook
max_main_replicas = "2"                                       # Max number of runner PODs . Do not confuse with Nodes
min_main_replicas = "1"                                       # Min number of runner PODs . Do not confuse with Nodes
webhook_scaling = "false"                                     # Enable webhook scaling. When disabled runner busy percentage is used
#state_bucket_name = "state_bucket_name"                      # Not used by terraform. This is just to reference what bucket is used for others
```
5. Make sure you set the bucket name in the comment in the environment file for documentation purposes

6.  From this directory, init terraform with:
```
terraform init -backend-config="bucket=bucket_name"
```
7. Terraform apply
```
terraform apply -var-file=environments/environment_name.env
```

# Maintanance

- To access the ARC k8s cluster call the `get_kubeconfig_command` terraform output and run the command

