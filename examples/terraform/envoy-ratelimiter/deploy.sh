#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script deploys the Envoy Rate Limiter on GKE.

set -e

COMMAND=${1:-"apply"}

# 1. Initialize Terraform
if [ ! -d ".terraform" ]; then
    echo "Initializing Terraform..."
    terraform init
else
    # Verify terraform initialization is valid, or re-initialize
    terraform init -upgrade=false >/dev/null 2>&1 || terraform init
fi

if [ "$COMMAND" = "destroy" ]; then
    echo "Destroying Envoy Rate Limiter Resources..."
    terraform destroy -auto-approve
    exit $?
fi

if [ "$COMMAND" = "apply" ]; then
    echo "Deploying Envoy Rate Limiter..."

    echo "--------------------------------------------------"
    echo "Creating/Updating GKE Cluster..."
    echo "--------------------------------------------------"
    # Deploy the cluster and wait for it to be ready.
    terraform apply -target=time_sleep.wait_for_cluster -auto-approve

    echo ""
    echo "--------------------------------------------------"
    echo "Deploying Application Resources..."
    echo "--------------------------------------------------"
    # Deploy the rest of the resources
    terraform apply -auto-approve

    echo ""
    echo "Deployment Complete!"
    echo "Cluster Name: $(terraform output -raw cluster_name)"
    echo "Load Balancer IP: $(terraform output -raw load_balancer_ip)"
    exit 0
fi

echo "Usage:"
echo "  ./deploy.sh [apply]    # Initialize and deploy resources (Default)"
echo "  ./deploy.sh destroy    # Destroy resources"
exit 1
