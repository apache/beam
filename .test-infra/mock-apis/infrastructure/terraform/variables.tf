// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

variable "project" {
  type        = string
  description = "The Google Cloud (GCP) project ID within which this module provisions resources"
}

variable "region" {
  type        = string
  description = "The Compute region which which this module provisions resources"
}

variable "resource_name_prefix" {
  type        = string
  description = "The prefix to apply when naming resources followed by a random string"
}

variable "router" {
  type        = string
  description = "The name of the Compute Network Router"
}

variable "network" {
  type        = string
  description = "The Virtual Private Cloud (VPC) network ID"
}

variable "subnetwork" {
  type        = string
  description = "The Virtual Private Cloud (VPC) subnetwork ID"
}
