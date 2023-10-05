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

package common

import "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/environment"

var (
	// Port is the port to bind a service.
	Port environment.Variable = "PORT"

	// CacheHost is the host address of the cache.
	CacheHost environment.Variable = "CACHE_HOST"

	// QuotaId uniquely identifies a quota measure.
	QuotaId environment.Variable = "QUOTA_ID"

	// QuotaSize specifies the size of the quota.
	QuotaSize environment.Variable = "QUOTA_SIZE"

	// QuotaRefreshInterval configures how often a quota is refreshed.
	QuotaRefreshInterval environment.Variable = "QUOTA_REFRESH_INTERVAL"
)
