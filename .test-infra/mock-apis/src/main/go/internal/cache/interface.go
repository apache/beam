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

package cache

import (
	"context"
	"time"
)

// HealthChecker checks the health and availability of a resource.
type HealthChecker interface {

	// Alive checks whether the resource is healthy and available.
	Alive(ctx context.Context) error
}

// UInt64Setter associates a key with a value for an expiry time.Duration.
type UInt64Setter interface {
	HealthChecker

	// Set a key with a value for an expiry time.Duration.
	Set(ctx context.Context, key string, value uint64, expiry time.Duration) error
}

// Decrementer decrements a value associated with a key.
type Decrementer interface {
	HealthChecker

	// Decrement the value associated with a key; returns the value after
	// decrementing it.
	Decrement(ctx context.Context, key string) (int64, error)
}
