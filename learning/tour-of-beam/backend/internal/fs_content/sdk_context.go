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

package fs_content

import (
	"log"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

// Common SDK-wide context
type sdkContext struct {
	sdk tob.Sdk

	idsWatcher idsWatcher
}

func newSdkContext(sdk tob.Sdk) *sdkContext {
	return &sdkContext{
		sdk,
		idsWatcher{make(map[string]struct{})},
	}
}

// Watch for duplicate ids. Not thread-safe!
type idsWatcher struct {
	ids map[string]struct{}
}

func (w *idsWatcher) CheckId(id string) {
	if _, exists := w.ids[id]; exists {
		log.Fatalf("Duplicate id: %v", id)
	}
	w.ids[id] = struct{}{}
}
