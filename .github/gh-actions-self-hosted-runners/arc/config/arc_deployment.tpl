#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
apiVersion: actions.summerwind.dev/v1alpha1
kind: RunnerDeployment
metadata:
  name: ${name}
spec:
  template:
    metadata:
      annotations:
        cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
    spec:
      dockerMTU: 1460
      %{~ if selector == true  ~}
      nodeSelector:
        runner-pool: ${name} 
      %{~ endif ~}
      %{~ if taint == true  ~}
      tolerations:
        - key: "runner-pool"
          operator: "Equal"
          value: ${name}
          effect: "NoSchedule"
      %{~ endif ~}
      image: ${image}
      organization: ${organization}
      group: "${group}"
      labels:
      %{~ for label in labels ~}
        - ${label}
      %{~ endfor ~}
      env:
      - name: POD_UID
        valueFrom:
          fieldRef:
            fieldPath: metadata.uid
      resources:
        requests:
          cpu: ${requests.cpu}
          memory: ${requests.memory}
        limits:
      %{~ if limits.cpu != "" ~}
          cpu: ${limits.cpu}
      %{~ if limits.memory != "" ~}
          memory: ${limits.memory}
      %{~ endif ~}
      %{~ endif ~}

