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

// Package hooksx defines user-facing entrypoints into Beam hooks, providing
// input validation and removing potential errors arising from typos in hook
// names. This also allows hook failures to occur earlier when the hook is
// enabled, not when the hooks are called. All hooks that accept input from
// users should have hooksx functions associated with them.
package hooksx

import (
	"fmt"
	"strconv"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

// EnableSideInputCache accepts a desired maximum size for the side input cache, validates that
// it is non-negative, then formats the integer as a string to pass to EnableHook. A size of 0
// disables the cache. This hook is utilized in pkg/beam/core/runtime/harness/harness.go when the
// cache is initialized.
func EnableSideInputCache(size int64) {
	if size < 0 {
		panic(fmt.Sprintf("side input cache size must be greater than or equal to 0, got %v", size))
	}
	sizeString := strconv.FormatInt(size, 10)
	hooks.EnableHook("EnableSideInputCache", sizeString)
}
