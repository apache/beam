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

// Package harnessopts defines user-facing entrypoints into Beam hooks affecting
// the SDK harness, providing input validation and removing potential errors
// arising from typos in hook names. This also allows hook failures to occur earlier
// when the hook is  enabled, not when the hooks are called. All harness-defined hooks
// that accept input from users should have harnessopts functions associated with them.
package harnessopts

import (
	"fmt"
	"strconv"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

const (
	cacheCapacityHook = "beam:go:hook:sideinputcache:capacity"
)

// SideInputCacheCapacity accepts a desired capacity for the side input cache. A non-zero positive
// integer enables the cache (the capacity of the cache is 0 by default.) Cache use also requires runner
// support.
func SideInputCacheCapacity(capacity int64) error {
	if capacity < 0 {
		return fmt.Errorf("capacity of cache cannot be negative, got %v", capacity)
	}
	capString := strconv.FormatInt(capacity, 10)
	// The hook itself is defined in beam/core/runtime/harness/cache_hooks.go
	return hooks.EnableHook(cacheCapacityHook, capString)
}
