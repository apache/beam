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

This module provisions a Bitnami Kafka Cluster based on its
[helm chart](https://github.com/bitnami/charts/tree/main/bitnami/kafka).
It uses the [terraform helm provider](https://registry.terraform.io/providers/hashicorp/helm/latest/docs)
Therefore, you DO NOT need helm to apply this module.

# Requirements

- [terraform](https://terraform.io)
- Connection to a kubernetes cluster (See: [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine/google-kubernetes-engine](../../terraform/google-cloud-platform/google-kubernetes-engine) in this repository)
- [kubectl](https://kubernetes.io/docs/reference/kubectl/) cli

# Usage

Simply follow standard terraform workflow to apply this module.

```
terraform init
terraform apply
```

# Special note about GKE Autopilot

When applying this module to
[Google Kubernetes Engine (GKE) Autopilot](https://cloud.google.com/kubernetes-engine/docs/concepts/autopilot-overview)
you will see an "Unschedulable" status. This is because, GKE Autopilot
needs time to scale up the node. After some time, the kubernetes cluster
will provision the kafka cluster when these compute resources are available.

# Debugging and Troubleshooting

This module deploys a kafka client on the cluster to help with debugging and
troubleshooting.

## Query the kafka client pod name

Run the following command to query the pod name.

```
kubectl get po -l app=kafka-client
```

You should see something similar to the following:
```
NAME                           READY   STATUS    RESTARTS   AGE
kafka-client-cdc7c8885-nmcjc   1/1     Running   0          4m12s
```

## Get a shell to the running container

Run the following command to shell into the running container.

```
kubectl exec --stdin --tty kafka-client-cdc7c8885-nmcjc -- /bin/bash
```

## Execute kafka commands

The container executes using the latest [bitnami/kafka](https://hub.docker.com/r/bitnami/kafka/)
image. It has installed all the necessary `kafka-*.sh` scripts in its path.

In all of the commands, you can use the flag: `--bootstrap-server kafka:9092`
because the pod is in the same kubernetes cluster and takes advantage
of its domain name service (DNS). The bitnami helm operator creates a Kubernetes
service called `kafka` that exposes port `9092`.

### Get the cluster-id

The following command gives you the cluster ID and validates you can connect to
the cluster.

```
kafka-cluster.sh cluster-id --bootstrap-server kafka:9092
```

### Create a topic

The following command creates a Kafka topic.

```
kafka-topics.sh --create --topic some-topic --partitions 3 --replication-factor 3 --bootstrap-server kafka:9092
```

### Get information about a topic

The following command queries information about a topic
(assuming the name `some-topic`).

```
kafka-topics.sh --describe --topic some-topic --bootstrap-server kafka:9092
```

See https://kubernetes.io/docs/tasks/debug/debug-application/get-shell-running-container/
