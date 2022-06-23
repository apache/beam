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

function retry() {
  local command=$1
  local max_retries=$2
  local sleep_time=$3

  for ((i = 1; i <= max_retries; i++)); do
    local output
    output=$(eval "${command}")

    local status=$?

    if [[ ${status} == 0 ]] && [[ -n  ${output} ]]; then
      echo "${output}"
      return 0
    fi

    if [[ $i == "${max_retries}" ]]; then
      echo "Command failed after ${max_retries} retries" >&2
      return 1
    fi

    sleep "${sleep_time}"
  done
}

# Invokes "kubectl apply" using specified kubeconfig and namespace.
#
# Usage: ./kubernetes.sh apply <path to .yaml file or directory with .yaml files>
function apply() {
  eval "$KUBECTL apply -R -f $1"
}

# Invokes "kubectl delete" using specified kubeconfig and namespace.
#
# Usage: ./kubernetes.sh delete <path to .yaml file>
function delete() {
  eval "$KUBECTL delete -R -f $1"
}

# Creates a namespace.
#
# Usage: ./kubernetes.sh createNamespace <namespace name>
function createNamespace() {
  eval "kubectl --kubeconfig=${KUBECONFIG} create namespace $1"
}

# Deletes whole namespace with all its contents.
#
# Usage: ./kubernetes.sh deleteNamespace <namespace name>
function deleteNamespace() {
  eval "kubectl --kubeconfig=${KUBECONFIG} delete namespace $1"
}

# Gets Load Balancer Ingress IP address.
# Blocks and retries until the IP is present or retry limit is exceeded.
#
# Usage: ./kubernetes.sh loadBalancerIP <name of the load balancer service>
function loadBalancerIP() {
  local name=$1
  local command="$KUBECTL get svc $name -ojsonpath='{.status.loadBalancer.ingress[0].ip}'"
  retry "${command}" 36 10
}

# Gets Available NodePort to avoid conflicts with already allocated ports
#
# Usage: ./kubernetes.sh getAvailablePort <low range port> <high range port>
function getAvailablePort() {
  local lowRangePort=$(($1 + 1)) #not inclusive low range
  local highRangePort=$2
  local used=false
  local command="$KUBECTL get svc --all-namespaces -o \
  go-template='{{range .items}}{{range.spec.ports}}{{if .nodePort}}{{.nodePort}}{{\"\n\"}}{{end}}{{end}}{{end}}'"
  local usedPorts
  usedPorts=$(eval "${command}")
  local availablePort=$lowRangePort

  for i in $(seq $lowRangePort $highRangePort);
    do
      while IFS= read -r usedPort; do
        if [ "$i" = "$usedPort" ]; then
          used=true
          break
        fi
      done <<< "$usedPorts"
    if $used; then
      availablePort=$((availablePort + 1))
      used=false
    else
      echo $availablePort
      return 0
    fi
    echo $availablePort
  done
}
#Waits until a designated job finish to continue the workflow execution
#Usage: ./kubernetes.sh waitForJob <kubernetes job name>  <timeout i.e: 30m, 20s etc.>
function waitForJob(){
  echo "Waiting for job completion..."
  jobName=$1 
  eval "$KUBECTL wait --for=condition=complete --timeout=$2 $jobName" 
  echo "Job completed"
}

"$@"
