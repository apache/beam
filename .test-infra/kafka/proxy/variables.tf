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

variable "project" {
  type        = string
  description = "The Google Cloud Platform (GCP) project within which resources are provisioned"
}

variable "region" {
  type        = string
  description = "The Google Cloud Platform (GCP) region in which to provision resources"
}

variable "network" {
  type        = string
  description = "The Google Cloud Virtual Private Cloud (VPC) network name"
}

variable "router" {
  type        = string
  description = "The name of the Google Compute Router resource associated with the VPC Network"
}

variable "router_nat" {
  type        = string
  description = "The name of the NAT associated with the Google Compute Router"
}

variable "subnetwork" {
  type        = string
  description = "The Google Cloud Virtual Private Cloud (VPC) subnetwork name"
}

variable "service_account_id" {
  type        = string
  description = "The ID of the service account to bind to the bastion host"
}

variable "kafka_proxy_version" {
  type        = string
  description = "The release version of https://github.com/grepplabs/kafka-proxy/releases"
}

variable "machine_type" {
  type        = string
  description = "The machine type to specify for the provision bastion host"
}

variable "bootstrap_endpoint_mapping" {
  type        = map(number)
  description = <<EOF
  The mapping of kafka bootstrap server endpoints i.e. 1.2.3.4:9094=0.0.0.0:9092 to assign bootstrap server mapping.
If Kafka is installed on Kubernetes, run `kubectl get svc` and acquire the list of load balanced bootstrap IP.
For example, the output below of `kubectl get svc` would require the 'Configuring as' shown following.

NAME    TYPE          CLUSTER-IP  EXTERNAL-IP   PORTS
kafka-0 LoadBalancer  34.1.2.3    10.1.2.3      tcp-external:9094►12345
kafka-1 LoadBalancer  34.1.2.4    10.1.2.4      tcp-external:9094►12346
kafka-2 LoadBalancer  34.1.2.5    10.1.2.5      tcp-external:9094►12347

Configuring as
{
  "10.1.2.3:9094": 9092,
  "10.1.2.4:9094": 9093,
  "10.1.2.5:9094": 9094,
}

yields when computing the kafka-proxy flags:
  10.1.2.3:9094=0.0.0.0:9092,
  10.1.2.4:9094=0.0.0.0:9093,
  10.1.2.5:9094=0.0.0.0:9094,
EOF
}

variable "allows_iap_ingress_cidr_range" {
  type        = string
  description = "The CIDR range that identity aware proxy (IAP) uses for TCP forwarding"
}
