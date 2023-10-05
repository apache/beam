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

// Package internal is where the less separable parts of the runner
// are put together in order to execute pipelines, and validate that
// beam features are implemented, and configured appropriately for
// the variant a pipeline is using.
//
// Importantly, it is the package that contains unit test pipelines
// to exercise beam features in a pipeline context, rather than
// simpler mechanical unit tests as in the sub packages.
package internal
