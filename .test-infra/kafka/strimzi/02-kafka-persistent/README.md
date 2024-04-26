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

This [kustomization](https://kubectl.docs.kubernetes.io/) installs the
strimzi kafka persistent cluster and redistributed from its source:
https://github.com/strimzi/strimzi-kafka-operator/blob/main/examples/kafka/kafka-persistent.yaml
The directories within [base](./base) contain the operator manifests and
are named according to the version release.

# Usage

Simply deploy the cluster by using kustomize plugin of kubectl
```
kubectl apply -k .test-infra/kafka/strimzi/02-kafka-persistent/overlays/gke-internal-load-balanced --namespace=strimzi
```
and wait until the cluster is deployed
```
kubectl wait kafka beam-testing-cluster --for=condition=Ready --timeout=1200s
```
Then to get the needed ips and ports:
```
kubectl get svc beam-testing-cluster-kafka-$REPLICA_NUMBER -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
kubectl get svc beam-testing-cluster-kafka-$REPLICA_NUMBER -o jsonpath='{.spec.ports[0].port}'
```
Where $REPLICA_NUMBER is value from 1 to 3 by default.