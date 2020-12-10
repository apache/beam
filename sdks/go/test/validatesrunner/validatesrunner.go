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

// Package validatesrunner contains Validates Runner tests, which are a type of
// integration test that execute short pipelines on various runners to validate
// runner behavior.
//
// These tests are intended to be used via "go test validatesrunner/...". Any
// flags usually necessary for running Go SDK pipelines should also be included,
// the notable flags being "--runner", "--endpoint", and "--expansion_addr".
package validatesrunner

import "flag"

var (
	// expansionAddr is the endpoint for an expansion service for cross-language
	// transforms.
	expansionAddr = flag.String("expansion_addr", "", "Address of Expansion Service")
)
