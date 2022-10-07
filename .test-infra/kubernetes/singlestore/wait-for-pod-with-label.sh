#!/usr/bin/env bash
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Set of common operations that CI needs to invoke when using Kubernetes.
#    The operations can be invoked using a provided kubeconfig file
#    and kubernetes namespace.
#
#    Specify the following environment variables to override defaults:
#    - KUBECONFIG: path to .kube/config file (default: $HOME/.kube/config)
#    - KUBERNETES_NAMESPACE: namespace to be used (default: default)

set -euxo pipefail

KUBECONFIG="${KUBECONFIG:=$HOME/.kube/config}"
KUBERNETES_NAMESPACE="${KUBERNETES_NAMESPACE:=default}"
KUBECTL="kubectl --kubeconfig=$KUBECONFIG --namespace=$KUBERNETES_NAMESPACE"

podName=$1
timeout=$2

echo "Waiting for the Pod $(podName) ..."

SECONDS=0
until [ "$(${KUBECTL} get pods -o=jsonpath="{.items[?(@.metadata.labels.name=='${podName}')]['status.phase']}")" == "Running" ]
do
  if (( SECONDS > timeout ))
  then
   echo "Timed out waiting for the Pod $(podName)"
   return 1
  fi

  sleep 1
done

echo "Pod $(podName) is running"
