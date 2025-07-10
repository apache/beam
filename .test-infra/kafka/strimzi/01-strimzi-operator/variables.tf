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

variable "kubeconfig_path" {
  type        = string
  description = "The path to the Kube config file; See https://kubernetes.io/docs/concepts/configuration/organize-cluster-access-kubeconfig/"
}

variable "name" {
  type        = string
  description = "The name of the Kafka cluster"
}

variable "namespace" {
  type        = string
  description = "The namespace to provision the Kubernetes workload"
}

variable "chart_name" {
  type        = string
  description = "The name of the Helm chart"
}

variable "chart_repository" {
  type        = string
  description = "The URL of the chart respository"
}

variable "chart_version" {
  type        = string
  description = "The version of the Helm chart"
}
