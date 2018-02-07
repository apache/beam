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

package exec

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// MainInput is the main input and is unfolded in the invocation, if present.
type MainInput struct {
	Key    FullValue
	Values []ReStream
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func Invoke(ctx context.Context, fn *funcx.Fn, opt *MainInput, extra ...interface{}) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}

	// (1) Populate contexts

	args := make([]interface{}, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = ctx
	}

	// (2) Main input from value, if any.

	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter | funcx.FnEmit)
	i := 0

	if opt != nil {
		if index, ok := fn.EventTime(); ok {
			args[index] = opt.Key.Timestamp
		}

		args[in[i]] = Convert(opt.Key.Elm, fn.Param[in[i]].T)
		i++
		if opt.Key.Elm2 != nil {
			args[in[i]] = Convert(opt.Key.Elm2, fn.Param[in[i]].T)
			i++
		}

		for _, iter := range opt.Values {
			param := fn.Param[in[i]]

			if param.Kind != funcx.FnIter {
				return nil, fmt.Errorf("GBK/CoGBK result values must be iterable: %v", param)
			}

			// TODO(herohde) 12/12/2017: allow form conversion on GBK results?

			it := makeIter(param.T, iter)
			it.Init()
			args[in[i]] = it.Value()
			i++
		}
	}

	// (3) Precomputed side input and emitters (or other output).

	for _, arg := range extra {
		args[in[i]] = arg
		i++
	}

	// (4) Invoke

	ret, err := reflectx.CallNoPanic(fn.Fn, args)
	if err != nil {
		return nil, err
	}
	if index, ok := fn.Error(); ok && ret[index] != nil {
		return nil, ret[index].(error)
	}

	// (5) Return direct output, if any.

	out := fn.Returns(funcx.RetValue)
	if len(out) > 0 {
		value := &FullValue{}
		if index, ok := fn.OutEventTime(); ok {
			value.Timestamp = ret[index].(typex.EventTime)
		}

		value.Elm = ret[out[0]]
		if len(out) > 1 {
			value.Elm2 = ret[out[1]]
		}
		return value, nil
	}

	return nil, nil
}

func makeSideInputs(fn *funcx.Fn, in []*graph.Inbound, side []ReStream) ([]ReusableInput, error) {
	if len(side) == 0 {
		return nil, nil // ok: no side input
	}

	if len(in) != len(side)+1 {
		return nil, fmt.Errorf("found %v inbound, want %v", len(in), len(side)+1)
	}
	param := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	if len(param) <= len(side) {
		return nil, fmt.Errorf("found %v params, want >%v", len(param), len(side))
	}

	// The side input are last of the above params, so we can compute the offset easily.
	offset := len(param) - len(side)

	var ret []ReusableInput
	for i := 0; i < len(side); i++ {
		s, err := makeSideInput(in[i+1].Kind, fn.Param[param[i+offset]].T, side[i])
		if err != nil {
			return nil, fmt.Errorf("failed to make side input %v: %v", i, err)
		}
		ret = append(ret, s)
	}
	return ret, nil
}

func makeEmitters(fn *funcx.Fn, nodes []Node) ([]ReusableEmitter, error) {
	if len(nodes) == 0 {
		return nil, nil // ok: no output nodes
	}

	offset := 0
	if len(fn.Returns(funcx.RetValue)) > 0 {
		offset = 1
	}
	out := fn.Params(funcx.FnEmit)
	if len(out) != len(nodes)-offset {
		return nil, fmt.Errorf("found %v emitters, want %v", len(out), len(nodes)-offset)
	}

	var ret []ReusableEmitter
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		ret = append(ret, makeEmit(param.T, nodes[i+offset]))
	}
	return ret, nil
}

// makeSideInput returns a reusable side input of the given kind and type.
func makeSideInput(kind graph.InputKind, t reflect.Type, values ReStream) (ReusableInput, error) {
	switch kind {
	case graph.Singleton:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return nil, err
		}
		if len(elms) != 1 {
			return nil, fmt.Errorf("singleton side input %v for %v ill-defined", kind, t)
		}
		return &fixedValue{val: Convert(elms[0].Elm, t)}, nil

	case graph.Slice:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return nil, err
		}
		slice := reflect.MakeSlice(t, len(elms), len(elms))
		for i := 0; i < len(elms); i++ {
			slice.Index(i).Set(reflect.ValueOf(Convert(elms[i].Elm, t.Elem())))
		}
		return &fixedValue{val: slice.Interface()}, nil

	case graph.Iter:
		return makeIter(t, values), nil

	case graph.ReIter:
		return makeReIter(t, values), nil

	default:
		panic(fmt.Sprintf("Unexpected side input kind: %v", kind))
	}
}
