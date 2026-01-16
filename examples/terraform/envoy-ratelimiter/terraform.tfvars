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

project_id = "PROJECT_ID"
region     = "REGION"

vpc_name    = "VPC_NAME"
subnet_name = "SUBNET_NAME"

# update the below config value to match your need
# https://github.com/envoyproxy/ratelimit?tab=readme-ov-file#examples
ratelimit_config_yaml = <<EOF
domain: mongo_cps
descriptors:
  - key: database
    value: users
    rate_limit:
      unit: second
      requests_per_unit: 1
EOF

# Optional Resource Limits
# ratelimit_resources = {
#   requests = { cpu = "100m", memory = "128Mi" }
#   limits   = { cpu = "500m", memory = "512Mi" }
# }
