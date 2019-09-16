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

// Package optimized contains type-specialized shims for faster execution.
package optimized

// NOTE(herohde) 1/18/2018: omit []byte for encodes to avoid re-registration due to symmetry.

// TODO(herohde) 1/17/2018: we need a more convenient way to generate specialized Funcs of
// various signatures.

//go:generate specialize --package=optimized --input=callers.tmpl --x=data,universals --imports=typex
//go:generate specialize --package=optimized --input=decoders.tmpl --x=data,universals --imports=typex
//go:generate specialize --package=optimized --input=emitters.tmpl --x=data,universals --y=data,universals
//go:generate specialize --package=optimized --input=encoders.tmpl --x=primitives,universals --imports=typex
//go:generate specialize --package=optimized --input=inputs.tmpl --x=data,universals --y=data,universals
//go:generate gofmt -w callers.go decoders.go emitters.go encoders.go inputs.go
