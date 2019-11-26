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

package beam_test

import (
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

// foolFn is a no-op CombineFn.
type foolFn struct {
	OutputType beam.EncodedType
}

type foolAccum struct{}

func (f *foolFn) CreateAccumulator() *foolAccum {
	return &foolAccum{}
}

func (f *foolFn) AddInput(a *foolAccum, v beam.U) *foolAccum {
	return a
}

func (f *foolFn) MergeAccumulators(a *foolAccum, b *foolAccum) *foolAccum {
	return a
}

func (f *foolFn) ExtractOutput(a *foolAccum) beam.V {
	return reflect.New(f.OutputType.T).Elem().Interface()
}

func TestCombineWithTypeDefinition(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	in := beam.Create(s, 1, 2, 3)
	strType := reflect.TypeOf("")
	combineFn := &foolFn{OutputType: beam.EncodedType{T: strType}}
	output := beam.Combine(s, combineFn, in, beam.TypeDefinition{Var: beam.VType, T: strType})
	if output.Type().Type() != strType {
		t.Errorf("expect combine output type to be %v, got %v", strType, output.Type().Type())
	}
}
