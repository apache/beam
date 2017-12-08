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

package beam

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// External defines a Beam external transform. The interpretation of this primitive is runner
// specific. The runner is responsible for parsing the payload based on the
// spec provided to implement the behavior of the operation. Transform
// libraries should expose an API that captures the user's intent and serialize
// the payload as a byte slice that the runner will deserialize.
func External(s Scope, spec string, payload []byte, in []PCollection, out []reflect.Type) []PCollection {
	return MustN(TryExternal(s, spec, payload, in, out))
}

// TryExternal attempts to perform the work of External, returning an error indicating why the operation
// failed. Failure reasons include the use of side inputs, or an external transform that has both inputs
// and outputs.
func TryExternal(s Scope, spec string, payload []byte, in []PCollection, out []reflect.Type) ([]PCollection, error) {
	switch {
	case len(in) == 0 && len(out) == 0:
		return []PCollection{}, fmt.Errorf("External node not well-formed: out and in both empty")
	case len(in) > 0 && len(out) > 0:
		return []PCollection{}, fmt.Errorf("External DoFns are not currently supported")
	case len(in) > 1:
		return []PCollection{}, fmt.Errorf("External operations with side inputs are not currently supported")
	case len(out) > 1:
		return []PCollection{}, fmt.Errorf("External operations with side outputs are not currently supported")
	case len(out) == 1:
		return tryExternalSource(s, spec, payload, out[0])
	case len(in) == 1:
		return tryExternalSink(s, in[0], spec, payload)
	}

	panic(fmt.Errorf("Impossible case: len[in]=%d, len[out]=%d", len(in), len(out)))
}

// TODO(wcn): the use of dynamic functions was a creative hack to minimize changes to the runtime
// while we design aspects of this feature. While the public API is locked down as above, the details
// here about how the payload is conveyed to the runner will certainly change, as this is a top-level
// primitive. Runners depending on this coding do so AT THEIR OWN RISK and will be broken when we convert
// this implementation to its final internal representation.

func tryExternalSource(s Scope, spec string, payload []byte, out reflect.Type) ([]PCollection, error) {
	if !s.IsValid() {
		return nil, fmt.Errorf("invalid scope")
	}
	emit := reflect.FuncOf([]reflect.Type{out}, nil, false)
	fnT := reflect.FuncOf([]reflect.Type{emit}, []reflect.Type{reflectx.Error}, false)

	gen := makeGenWithErrorMessage("ExternalSource node cannot be directly executed")
	g := &graph.DynFn{Name: "ExternalSource", Data: payload, T: fnT, Gen: gen}
	fn, err := graph.NewDoFn(g)

	if err != nil {
		return []PCollection{}, err
	}
	edge, err := graph.NewSource(s.real, s.scope, fn, nil)
	if err != nil {
		return []PCollection{}, err
	}
	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return []PCollection{ret}, nil
}

func tryExternalSink(s Scope, in PCollection, spec string, payload []byte) ([]PCollection, error) {
	if !s.IsValid() {
		return nil, fmt.Errorf("invalid scope")
	}
	if !in.IsValid() {
		return []PCollection{}, fmt.Errorf("invalid main pcollection")
	}

	fnT := reflect.FuncOf([]reflect.Type{typex.SkipW(in.n.Type()).Type()}, []reflect.Type{reflectx.Error}, false)
	gen := makeGenWithErrorMessage("ExternalSink node cannot be directly executed")
	g := &graph.DynFn{Name: "ExternalSink", Data: payload, T: fnT, Gen: gen}
	fn, err := graph.NewDoFn(g)

	if err != nil {
		return []PCollection{}, err
	}
	_, err = graph.NewSink(s.real, s.scope, fn, in.n)
	if err != nil {
		return []PCollection{}, err
	}
	return []PCollection{}, nil
}

func makeGenWithErrorMessage(msg string) func([]byte) func([]reflect.Value) []reflect.Value {
	return func(in []byte) func([]reflect.Value) []reflect.Value {
		return func(args []reflect.Value) []reflect.Value {
			ret := reflect.ValueOf(fmt.Errorf(msg))
			return []reflect.Value{ret.Convert(reflectx.Error)}
		}
	}
}
