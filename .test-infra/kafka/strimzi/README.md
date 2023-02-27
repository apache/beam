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

This folder provisions a [strimzi.io](https://strimzi.io) kafka cluster on kubernetes.  Ingress assumes a
Google Kubernetes Engine cluster.

# Requirements

See [requirements](../README.md) for details on requirements.

# Usage

Run the following commands to provision the kafka cluster.  Each command needs to wait until the previous completes.

## 1. Create the namespace.

```
kubectl apply -f 00-namespace.yaml
```

## 2. Create the strimzi operator.

```
kubectl apply -f 01-strimzi-operator.yaml
```

Wait until completion before proceeding to the next step.

```
 kubectl get deploy strimzi-cluster-operator --namespace strimzi -w
```

## 3. Create the kafka cluster.

```
kubectl apply -f 02-kafka-persistent.yaml
```

You can watch while all the resources are created.

```
kubectl get all --namespace strimzi
```