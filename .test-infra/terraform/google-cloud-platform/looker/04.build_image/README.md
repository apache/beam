# Overview

# Usage

```
ARTIFACT_REGISTRY_URL=$(terraform -chdir=01.setup output -raw artifact_registry_url)
```

```
LOOKER_JARS_GLOB=$(terraform -chdir=03.jars output -raw looker_jars_glob)
```

```
DIR=04.build_image; \
terraform -chdir=$DIR init; \
terraform -chdir=$DIR apply -var-file=common.tfvars -var=artifact_registry_url=$ARTIFACT_REGISTRY_URL -var=looker_jars_glob=$LOOKER_JARS_GLOB -var=project=$(gcloud config get-value project)
```