# Overview

This directory holds files for deploying to Kubernetes.

# Requirements

The instructions assume:
- Existing Kubernetes cluster; see [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine](../../../../../../.test-infra/terraform/google-cloud-platform/google-kubernetes-engine)
- Artifact Registry; see [.test-infra/terraform/google-cloud-platform/artifact-registry](../../../../../../.test-infra/terraform/google-cloud-platform/artifact-registry)
- [ko](https://ko.build)

# Steps

## Step 1

Set various environment variables.
```
REGION=
PROJECT=
ARTIFACT_REGISTRY_ID=
```

Set [ko](https://ko.build) environment variables.

```
export KO_DOCKER_REPO=$REGION-docker.pkg.dev/$PROJECT/$ARTIFACT_REGISTRY_ID
export KO_CONFIG_PATH=go/cmd/wasmx/.ko.yaml
```

## Step 2

Deploy infrastructure. *NOTE the correct directory from which to execute this command.*

```
cd sdks
kubectl kustomize ./go/cmd/wasmx/infrastructure/k8s | ko resolve -f - | kubectl apply -f -
```

### What is the previous command doing?

`kubectl kustomize ./go/cmd/wasmx/infrastructure/k8s` resolves the [kustomization](https://kustomize.io)
which then pipes to `ko resolve -f -`. [ko](https://ko.build) finds any `ko://` URIs and build according to the
resolved go paths. Afterwhich, ko replaces these `ko://` with the built and pushed container images.
Finally 
