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

package testpipeline

import "github.com/apache/beam/sdks/go/pkg/beam"

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=testpipeline --identifiers=VFn,KvFn,KvEmitFn,SCombine
//go:generate go fmt

// VFn is a do nothing example function with a k and v.
func VFn(v int) (string, int) {
	return "key", v
}

// KvFn is a do nothing example function with a k and v.
func KvFn(k string, v int) (string, int) {
	return k, v
}

// KvEmitFn is a do nothing example function with a k and v that uses an emit
// instead of a return.
func KvEmitFn(k string, v int, emit func(string, int)) {
	emit(k, v)
}

// SCombine is a Do Nothing structural doFn to ensure that generating things for
// combinefn structs works.
type SCombine struct{}

// MergeAccumulators lifecycle method.
func (s *SCombine) MergeAccumulators(a, b int) int { return a + b }

// otherMethod should never have a shim generated for it.
// Unfortunately, outside of a manual inspection, or parsing of the
// generated file, this is difficult to test.
func (s *SCombine) otherMethod(v beam.V) beam.V {
	return v
}
