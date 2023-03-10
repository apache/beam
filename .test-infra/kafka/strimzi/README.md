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

# Overview

This folder provisions a [strimzi.io](https://strimzi.io) kafka cluster on kubernetes.
Each folder is named in the required order of application.
These resources are managed using Kustomize.
See https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/
for more details on how Kustomize works with Kubernetes.

# Requirements

See [requirements](../README.md) for details on requirements.

# Preview kustomization

To preview any of the kustomizations, run the following command.

```
kubectl kustomize <folder>
```

For example, to preview the kustomization for [01-strimzi-operator](01-strimzi-operator):

```
kubectl kustomize 01-strimzi-operator
```

# Usage

Run the following commands provision the kafka cluster and assumes
the working directory [.test-infra/kafka-strimzi](.).

**IMPORTANT: Each command needs to wait until the previous completes.**

## 1. Install the strimzi operator.

To preview the kustomization prior to appyling.

```
kubectl apply -k 01-strimzi-operator
```

Wait until completion before proceeding to the next step.

```
 kubectl get deploy strimzi-cluster-operator --namespace strimzi -w
```

## 3. Create the kafka cluster.

The kafka cluster specifics relies on kustomize overlays.

### GKE internal load balanced kafka cluster

The following command installs a Google Kubernetes Engine (GKE)
internally load balanced kafka cluster through the
[02-kafka-persistent/overlays/gke-internal-load-balanced](02-kafka-persistent/overlays/gke-internal-load-balanced)
overlay.

```
kubectl apply -k 02-kafka-persistent/overlays/gke-internal-load-balanced
```

You can watch while all the resources are created.

```
kubectl get all --namespace strimzi
```

## 4. Acquire kafka host and port.

After all kafka cluster resources are created, you can run the
following command to find the kafka host and port.

```
kubectl get svc beam-testing-cluster-kafka-external-bootstrap --namespace strimzi
```
