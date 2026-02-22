#!/bin/bash
set -e

COMMAND=${1:-"apply"}

if [ "$COMMAND" = "destroy" ]; then
    echo "Destroying Envoy Rate Limiter Resources..."
    echo "Note: If 'deletion_protection = true',this operation will fail for the cluster."
    terraform destroy
    exit $?
fi

if [ "$COMMAND" = "apply" ]; then
    # Auto-initialize if needed
    if [ ! -d ".terraform" ]; then
        echo "Initializing Terraform..."
        terraform init
    fi

    echo "Deploying Envoy Rate Limiter..."

    echo "--------------------------------------------------"
    echo "Creating GKE Cluster..."
    echo "--------------------------------------------------"
    # Deploy the cluster in step-1 before deploying the application resources.
    terraform apply -target=time_sleep.wait_for_cluster -auto-approve

    echo ""
    echo "--------------------------------------------------"
    echo "Deploying Application Resources..."
    echo "--------------------------------------------------"
    # Deploy the application resources in step-2.
    terraform apply -auto-approve

    echo ""
    echo "Deployment Complete!"
    echo "Cluster Name: $(terraform output -raw cluster_name)"
    echo "Load Balancer IP: $(terraform output -raw load_balancer_ip)"
    exit 0
fi

echo "Detailed Usage:"
echo "  ./deploy.sh [apply]    # Deploy resources (Default)"
echo "  ./deploy.sh destroy    # Destroy resources"
exit 1
