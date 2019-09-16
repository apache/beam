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

// Package coderx contains coders for primitive types that aren't included
// in the beam model.
package coderx

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=coderx --identifiers=encString,decString,encUint32,decUint32,encInt32,decInt32,encUint64,decUint64,encInt64,decInt64,encVarIntZ,decVarIntZ,encVarUintZ,decVarUintZ,encFloat,decFloat
//go:generate go fmt
