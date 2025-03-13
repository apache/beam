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

// Package environment provides helpers for interacting with environment variables.
package environment

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	// HttpPort is the port to bind an HTTP service.
	HttpPort Variable = "HTTP_PORT"

	// GrpcPort is the port to bind a gRPC service.
	GrpcPort Variable = "GRPC_PORT"

	// CacheHost is the host address of the cache.
	CacheHost Variable = "CACHE_HOST"

	// ProjectId is the ID of the Google Cloud host project.
	ProjectId Variable = "PROJECT_ID"

	// QuotaId uniquely identifies a quota measure.
	QuotaId Variable = "QUOTA_ID"

	// QuotaSize specifies the size of the quota.
	QuotaSize Variable = "QUOTA_SIZE"

	// QuotaRefreshInterval configures how often a quota is refreshed.
	QuotaRefreshInterval Variable = "QUOTA_REFRESH_INTERVAL"
)

// Variable defines an environment variable via a string type alias.
// Variable's string defaultValue assigns the system environment variable key.
type Variable string

// Default a default value to the system environment.
func (v Variable) Default(value string) error {
	if v.Missing() {
		return os.Setenv((string)(v), value)
	}
	return nil
}

// MustDefault a default value to the system environment. Panics on error.
func (v Variable) MustDefault(value string) {
	if err := v.Default(value); err != nil {
		panic(err)
	}
}

// Missing reports whether the system environment variable is an empty string.
func (v Variable) Missing() bool {
	return v.Value() == ""
}

// Key returns the system environment variable key.
func (v Variable) Key() string {
	return (string)(v)
}

// Value returns the system environment variable value.
func (v Variable) Value() string {
	return os.Getenv((string)(v))
}

// Int returns the system environment variable parsed as an int.
func (v Variable) Int() (int, error) {
	return strconv.Atoi(v.Value())
}

// UInt64 returns the system environment variable value parsed as a uint64.
func (v Variable) UInt64() (uint64, error) {
	return strconv.ParseUint(v.Value(), 10, 64)
}

// Duration returns the system environment variable value parsed as time.Duration.
func (v Variable) Duration() (time.Duration, error) {
	return time.ParseDuration(v.Value())
}

// KeyValue returns a concatenated string of the system environment variable's
// <key>=<defaultValue>.
func (v Variable) KeyValue() string {
	return fmt.Sprintf("%s=%s", (string)(v), v.Value())
}

// Missing reports as an error listing all Variable among vars that are
// not assigned in the system environment.
func Missing(vars ...Variable) error {
	var missing []string
	for _, v := range vars {
		if v.Missing() {
			missing = append(missing, v.KeyValue())
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("variables empty but expected from environment: %s", strings.Join(missing, "; "))
	}
	return nil
}
