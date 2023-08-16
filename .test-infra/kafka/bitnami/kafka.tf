/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Provision the kafka cluster using the Bitnami helm chart.
resource "helm_release" "kafka" {
  wait = false
  repository = "https://charts.bitnami.com/bitnami"
  chart = "kafka"
  name  = "kafka"
  set {
    name  = "listeners.client.protocol"
    value = "PLAINTEXT"
  }
  set {
    name = "listeners.interbroker.protocol"
    value = "PLAINTEXT"
  }
  set {
    name = "listeners.external.protocol"
    value = "PLAINTEXT"
  }
  set {
    name = "externalAccess.enabled"
    value = "true"
  }
  set {
    name = "externalAccess.autoDiscovery.enabled"
    value = "true"
  }
  set {
    name = "rbac.create"
    value = "true"
  }
  set {
    name = "service.annotations"
    value = yamlencode({"networking.gke.io/load-balancer-type": "Internal" })
  }
  set {
    name = "externalAccess.service.broker.ports.external"
    value = "9094"
  }
  set {
    name = "externalAccess.service.controller.containerPorts.external"
    value = "9094"
  }
  set_list {
    name = "externalAccess.controller.service.loadBalancerAnnotations"
    value = [
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
    ]
  }
  set_list {
    name = "externalAccess.broker.service.loadBalancerAnnotations"
    value = [
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
      yamlencode({"networking.gke.io/load-balancer-type": "Internal" }),
    ]
  }
}

// Provision a kafka client to validate and debug the cluster.
resource "kubernetes_deployment" "kafka_client" {
  wait_for_rollout = false
  metadata {
    name = "kafka-client"
    labels = {
      app = "kafka-client"
    }
  }
  spec {
    selector {
      match_labels = {
        app = "kafka-client"
      }
    }
    template {
      metadata {
        labels = {
          app = "kafka-client"
        }
      }
      spec {
        container {
          name = "kafka-client"
          image = "bitnami/kafka:latest"
          image_pull_policy = "IfNotPresent"
          command = ["/bin/bash"]
          args = [
            "-c",
            "while true; do sleep 2; done",
          ]
        }
      }
    }
  }
}