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

// Package runtime contains runtime hooks and utilities for pipeline options
// and type registration. Most functionality done in init and hence is
// available both during pipeline-submission and at runtime.
package runtime

var (
	hooks       []func()
	initialized bool
)

// RegisterInit registers an Init hook. Hooks are expected to be able to
// figure out whether they apply on their own, notably if invoked in a remote
// execution environment. They are all executed regardless of the runner.
func RegisterInit(hook func()) {
	if initialized {
		panic("Init hooks have already run. Register hook during init() instead.")
	}

	hooks = append(hooks, hook)
}

// Init is the hook that all user code must call after flags processing and
// other static initialization, for now.
func Init() {
	initialized = true
	for _, hook := range hooks {
		hook()
	}
}

// Initialized exposes the initialization status for runners.
func Initialized() bool {
	return initialized
}
