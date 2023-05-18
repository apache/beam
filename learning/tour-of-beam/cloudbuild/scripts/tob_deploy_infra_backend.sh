#!/usr/bin/env bash

export DEBIAN_FRONTEND=noninteractive

apt-get -qq update

apt-get -qq install -y wget unzip software-properties-common git curl apt-transport-https ca-certificates gnupg jq lsb-release

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list

wget -nv https://releases.hashicorp.com/terraform/1.4.5/terraform_1.4.5_linux_amd64.zip 

unzip terraform_1.4.5_linux_amd64.zip

mv terraform /usr/local/bin/terraform

apt-get -qq update

apt-get -qq install -y google-cloud-sdk-gke-gcloud-auth-plugin google-cloud-sdk openjdk-11-jdk kubectl docker-ce golang

git clone --branch $BRANCH_NAME $REPO_NAME --single-branch

gcloud auth configure-docker $PG_REGION-docker.pkg.dev

gcloud container clusters get-credentials --region $PG_GKE_ZONE $PG_GKE_NAME --project $PROJECT_ID

cd beam/learning/tour-of-beam/terraform

gcloud datastore indexes create ../backend/internal/storage/index.yaml

echo "---- ENV OUTPUT---"
env | grep TF_VAR

terraform init -backend-config='bucket=$STATE_BUCKET'

terraform apply -auto-approve -var "pg_router_host=$(kubectl get svc -l app=backend-router-grpc -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}')"

echo "---- UPLOAD LEARNING METERIALS ---"
go run ../backend/cmd/ci_cd/ci_cd.go