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

# ------------------------------------------------------------------------------
# REQUIRED VARIABLES
# ------------------------------------------------------------------------------

variable "project_id" {
  description = "The Google Cloud Project ID"
  type        = string
}

variable "vpc_name" {
  description = "The name of the existing VPC network"
  type        = string
}

variable "subnet_name" {
  description = "The name of the existing subnetwork."
  type        = string
}

variable "ratelimit_config_yaml" {
  description = "Content of the ratelimit config.yaml (Required)"
  type        = string
}

# ------------------------------------------------------------------------------
# OPTIONAL VARIABLES
# ------------------------------------------------------------------------------

variable "region" {
  description = "The region to deploy resources to"
  type        = string
  default     = "us-central1"
}

variable "control_plane_cidr" {
  description = "CIDR block for GKE control plane"
  type        = string
  default     = "172.16.0.0/28"
}

variable "cluster_name" {
  description = "The name of the GKE cluster"
  type        = string
  default     = "ratelimit-cluster"
}

variable "deletion_protection" {
  description = "Whether to enable deletion protection on the GKE cluster. Set to true for production."
  type        = bool
  default     = false
}

variable "ip_name" {
  description = "The name of the static IP address to reserve. If empty, defaults to <cluster_name>-ratelimit-ip"
  type        = string
  default     = ""
}

variable "ratelimit_replicas" {
  description = "Number of replicas for the rate limit deployment"
  type        = number
  default     = 1
}

variable "min_replicas" {
  description = "Minimum number of replicas for rate limit deployment"
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Maximum number of replicas for rate limit deployment"
  type        = number
  default     = 100
}

variable "hpa_cpu_target_percentage" {
  description = "Target CPU utilization percentage for autoscaling"
  type        = number
  default     = 75
}

variable "hpa_memory_target_percentage" {
  description = "Target Memory utilization percentage for autoscaling"
  type        = number
  default     = 75
}

variable "ratelimit_image" {
  description = "Docker image for Envoy Rate Limit service"
  type        = string
  default     = "envoyproxy/ratelimit:e9ce92cc" 
}

variable "redis_image" {
  description = "Docker image for Redis"
  type        = string
  default     = "redis:6.2-alpine"
}

variable "statsd_exporter_image" {
  description = "Docker image for StatsD Exporter"
  type        = string
  default     = "prom/statsd-exporter:v0.24.0"
}

variable "ratelimit_log_level" {
  description = "Log level for ratelimit service"
  type        = string
  default     = "debug"
}

variable "ratelimit_grpc_max_connection_age" {
  description = "Duration a connection may exist before it will be closed by sending a GoAway"
  type        = string
  default     = "5m"
}

variable "ratelimit_grpc_max_connection_age_grace" {
  description = "Period after MaxConnectionAge after which the connection will be forcibly closed"
  type        = string
  default     = "1m"
}


variable "redis_resources" {
  description = "Compute resources for Redis container"
  type = object({
    requests = map(string)
    limits   = map(string)
  })
  default = {
    requests = {
      cpu    = "250m"
      memory = "128Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "512Mi"
    }
  }
}

variable "ratelimit_resources" {
  description = "Compute resources for Rate Limit container"
  type = object({
    requests = map(string)
    limits   = map(string)
  })
  default = {
    requests = {
      cpu    = "150m"
      memory = "128Mi"
    }
    limits = {
      cpu    = "500m"
      memory = "512Mi"
    }
  }
}