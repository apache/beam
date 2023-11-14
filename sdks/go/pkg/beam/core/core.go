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

// Package core contains constants and other static data related to the SDK,
// such as the SDK Name and version.
//
// As a rule, this package should not have dependencies, and should not depend
// on any package within the Apache Beam Go SDK.
//
// Files in this package may be generated or updated by release scripts, allowing
// for accurate version information to be included.
package core

const (
	// SdkName is the human readable name of the SDK for UserAgents.
	SdkName = "Apache Beam SDK for Go"
	// SdkVersion is the current version of the SDK.
	SdkVersion = "2.53.0.dev"

	// DefaultDockerImage represents the associated image for this release.
	DefaultDockerImage = "apache/beam_go_sdk:" + SdkVersion
)
