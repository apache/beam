#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"

   export "$KEY"="$VALUE"
done

export LOG_PATH=${LOG_PATH-"/dev/null"}
export GCP_PROJECT=${GCP_PROJECT}
export NETWORK_NAME=${NETWORK_NAME}
export SUBNETWORK_NAME=${SUBNETWORK_NAME}
export GKE_NAME=${GKE_NAME}
export PG_REGION=${PG_REGION}
export PG_LOCATION=${PG_LOCATION}
export STATE_BUCKET=${STATE_BUCKET}
export REDIS_NAME=${REDIS_NAME}
export MIN_COUNT=${MIN_COUNT}
export MAX_COUNT=${MAX_COUNT}
export GKE_MACHINE_TYPE=${GKE_MACHINE_TYPE}
export IP_ADDRESS_NAME=${IP_ADDRESS_NAME}
export REPOSITORY_ID=${REPOSITORY_ID}
export SERVICE_ACCOUNT_ID=${SERVICE_ACCOUNT_ID}
export ENVIRONMENT_NAME=${ENVIRONMENT_NAME}
export DNS_NAME=${DNS_NAME}


# This function logs the given message to a file and outputs it to the console.
function LogOutput ()
{
    echo "$(date --utc '+%D %T') $1" >> $LOG_PATH
    # CDLOG keyword to simplify search over the global log
    echo "DEPLOY_LOG $(date --utc '+%D %T') $1"
}

# Script starts in a clean environment in Cloud Build. Set minimal required environment variables
if [ -z "$PATH" ]; then
    export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin/:/usr/bin:/sbin:/bin"
fi
if [ -z "$HOME" ]; then
    export HOME="/builder/home"
fi

LogOutput "Installing dependencies"

apt update > /dev/null

# Install dependencies
apt install -y build-essential unzip apt-transport-https ca-certificates curl software-properties-common gnupg2 wget > /dev/null

# Install Docker
curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /dev/null
apt update > /dev/null && apt install -y docker-ce > /dev/null

#Install Helm
curl -fsSLo get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 > /dev/null
chmod +x get_helm.sh && ./get_helm.sh > /dev/null

# Install Terraform
wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
| tee /etc/apt/sources.list.d/hashicorp.list
apt update -y > /dev/null && apt install -y terraform > /dev/null

# Install kubectl
curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" \
&& chmod +x ./kubectl \
&& mv ./kubectl /usr/local/bin/kubectl

# Install jdk
apt install openjdk-11-jdk -y > /dev/null

mkdir playground/terraform/environment/$ENVIRONMENT_NAME

printf '%s\n' \
'project_id = "$GCP_PROJECT"' \
'network_name = "$NETWORK_NAME"' \
'subnetwork_name="$SUBNETWORK_NAME' \
'gke_name = "$GKE_NAME"' \
'region = "$PG_REGION"' \
'location = "$PG_LOCATION"' \
'state_bucket = "$STATE_BUCKET"' \
'redis_name = "$REDIS_NAME"' \
'min_count="$MIN_COUNT' \
'max_count = "$MAX_COUNT"' \
'gke_machine_type = "$GKE_MACHINE_TYPE"' \
'app_engine_flag = "$APP_ENGINE_FLAG"' \
'ip_address_name = "$IP_ADDRESS_NAME"' \
'repository_id = "$REPOSITORY_ID"' \
'service_account_id = "$SERVICE_ACCOUNT_ID"' \
> playground/terraform/environment/$ENVIRONMENT_NAME/terraform.tfvars

printf \
'bucket = "$STATE_BUCKET"'\
> playground/terraform/environment/$ENVIRONMENT_NAME/state.tfbackend
./gradlew playground:terraform:InitInfrastructure -Pproject_environment="$ENVIRONMENT_NAME" -Pdns-name="$DNS_NAME"
if [ $? -eq 0 ]
    then
        LogOutput "Deployed."
    else
        LogOutput "Not deployed"
fi
exit 0