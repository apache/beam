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

namespace = "api-overuse-study"

log_level = "debug"

cache_host = "redis-master:6379"

service_account_name = "api-overuse-study"

echo_service = {
  name  = "echo"
  port  = 8080
  image = "github.com/apache/beam/test-infra/pipelines/src/main/go/cmd/api_overuse_study/echo"
}

quota_service = {
  name  = "quota"
  port  = 8080
  image = "github.com/apache/beam/test-infra/pipelines/src/main/go/cmd/api_overuse_study/quota"
}

refresher_service_image = "github.com/apache/beam/test-infra/pipelines/src/main/go/cmd/api_overuse_study/refresher"
