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

- Connection to a kubernetes cluster (See: [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/google-kubernetes-engine](../../terraform/google-cloud-platform/google-kubernetes-engine) in this repository)
- [kubectl](https://kubernetes.io/docs/reference/kubectl/) cli

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

First install the strimzi operator:

```
kubectl apply -k 01-strimzi-operator
```

**IMPORTANT: Wait until completion before proceeding to the next step:**

```
 kubectl get deploy strimzi-cluster-operator --namespace strimzi -w
```

## 3. Create the kafka cluster.

A specific kafka cluster installation relies on kustomize overlays.

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

#### Test Kafka connection on local machine
After all kafka cluster resources are created, you will want to validate the
Kafka instance running on the kubernetes cluster.

In either of the commands you need to port forward the internal load
balancer service:

```
kubectl port-forward --namespace strimzi $(kubectl get pod --namespace strimzi --selector="strimzi.io/cluster=beam-testing-cluster,strimzi.io/kind=Kafka,strimzi.io/name=beam-testing-cluster-kafka" --output jsonpath='{.items[0].metadata.name}') 9094:9094
```

##### Simple telnet

In a new terminal after submitting the `kubectl port-forward` command above:

```
curl -v telnet://localhost:9094
```

You should see:
```
*   Trying 127.0.0.1:9094...
* Connected to localhost (127.0.0.1) port 9094 (#0)
```

##### Use kcat

See https://github.com/edenhill/kcat for instructions how to install `kcat`.

```
kcat -L -b localhost:9094
```

You should see something that looks like the following:
```
Metadata for all topics (from broker -1: localhost:9094/bootstrap):
 3 brokers:
  broker 0 at 10.128.0.12:9094 (controller)
  broker 2 at 10.128.0.13:9094
  broker 1 at 10.128.0.11:9094
```

#### Use with KafkaIO on Dataflow

After all kafka cluster resources are created, you can run the
following command to find the kafka host and port.

```
kubectl get svc beam-testing-cluster-kafka-external-bootstrap --namespace strimzi
```

You should see something like the following:

```
NAME                                            TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)          AGE
beam-testing-cluster-kafka-external-bootstrap   LoadBalancer   10.167.128.247   10.128.0.14   9094:31331/TCP   86m
```

Take note of the `EXTERNAL_IP` and `PORT`.  In the above example, we see
`10.128.0.14:9094`.  This is the bootstrap server.

Therefore, in the context of KafkaIO:

```
KafkaIO.read().withBootstrapServers("10.128.0.14:9094")
```

or

```
KafkaIO.write().withBootstrapServers("10.128.0.14:9094")
```
