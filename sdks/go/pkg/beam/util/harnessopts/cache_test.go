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

package harnessopts

import (
	"testing"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness" // Imports the cache hook
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

func TestSideInputCacheCapacity(t *testing.T) {
	err := SideInputCacheCapacity(1)
	if err != nil {
		t.Errorf("SideInputCacheCapacity failed when it should have succeeded, got %v", err)
	}
	ok, opts := hooks.IsEnabled(cacheCapacityHook)
	if !ok {
		t.Fatalf("SideInputCacheCapacity hook is not enabled")
	}
	if len(opts) != 1 {
		t.Errorf("num opts mismatch, got %v, want 1", len(opts))
	}
	if opts[0] != "1" {
		t.Errorf("cache size option mismatch, got %v, want %v", opts[0], 1)
	}
}

func TestSideInputCacheCapacity_Bad(t *testing.T) {
	err := SideInputCacheCapacity(-1)
	if err == nil {
		t.Errorf("SideInputCacheCapacity succeeded when it should have failed")
	}
}
