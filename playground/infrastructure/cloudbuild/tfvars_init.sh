#!/bin/bash
mkdir playground/terraform/environment/$_ENVIRONMENT_NAME
printf '%s\n' \
'project_id = "$PROJECT_ID"' \
'network_name = "$_NETWORK_NAME"' \
'gke_name = "$_GKE_NAME"' \
'region = "$LOCATION"' \
'location = "$LOCATION-a"' \
'state_bucket = "$_STATE_BUCKET"' \
'bucket_examples_name = "$_STATE_BUCKET-examples"' \
> playground/terraform/environment/$_ENVIRONMENT_NAME/terraform.tfvars
printf \
'bucket = "$_STATE_BUCKET"'\
> playground/terraform/environment/$_ENVIRONMENT_NAME/state.tfbackend