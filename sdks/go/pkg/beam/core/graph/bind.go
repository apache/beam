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

package graph

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// TODO(herohde) 4/21/2017: Bind is where most user mistakes will likely show
// up. We should verify that common mistakes yield reasonable errors.

// Bind returns the inbound, outbound and underlying output types for a Fn,
// when bound to the underlying input types. The complication of bind is
// primarily that UserFns have loose signatures and bind must produce valid
// type information for the execution plan.
//
// For example,
//
//     func (t EventTime, k typex.X, v int, emit func(string, typex.X))
// or
//     func (context.Context, k typex.X, v int) (string, typex.X, error)
//
// are UserFns that may take one or two incoming fulltypes: either KV<X,int>
// or X with a singleton side input of type int. For the purpose of the
// shape of data processing, the two forms are equivalent. The non-data types,
// context.Context and error, are not part of the data signature, but in play
// only at runtime.
//
// If either was bound to the input type [KV<string,int>], bind would return:
//
//     inbound:  [Main: KV<X,int>]
//     outbound: [KV<string,X>]
//     output:   [KV<string,string>]
//
// Note that it propagates the assignment of X to string in the output type.
//
// If either was instead bound to the input fulltypes [float, int], the
// result would be:
//
//     inbound:  [Main: X, Singleton: int]
//     outbound: [KV<string,X>]
//     output:   [KV<string, float>]
//
// Here, the inbound shape and output types are different from before.
func Bind(fn *funcx.Fn, typedefs map[string]reflect.Type, in ...typex.FullType) ([]typex.FullType, []InputKind, []typex.FullType, []typex.FullType, error) {
	addContext := func(err error, fn *funcx.Fn) error {
		return errors.WithContextf(err, "binding fn %v", fn.Fn.Name())
	}

	inbound, kinds, err := findInbound(fn, in...)
	if err != nil {
		return nil, nil, nil, nil, addContext(err, fn)
	}
	outbound, err := findOutbound(fn)
	if err != nil {
		return nil, nil, nil, nil, addContext(err, fn)
	}

	subst, err := typex.Bind(inbound, in)
	if err != nil {
		return nil, nil, nil, nil, addContext(err, fn)
	}
	for k, v := range typedefs {
		if substK, exists := subst[k]; exists {
			err := errors.Errorf("cannot substitute type %v with %v, already defined as %v", k, v, substK)
			return nil, nil, nil, nil, addContext(err, fn)
		}
		subst[k] = v
	}

	out, err := typex.Substitute(outbound, subst)
	if err != nil {
		return nil, nil, nil, nil, addContext(err, fn)
	}
	return inbound, kinds, outbound, out, nil
}

func findOutbound(fn *funcx.Fn) ([]typex.FullType, error) {
	ret := trimIllegal(returnTypes(funcx.SubReturns(fn.Ret, fn.Returns(funcx.RetValue)...)))
	params := funcx.SubParams(fn.Param, fn.Params(funcx.FnEmit)...)

	var outbound []typex.FullType

	// The direct output is the "main" output, if any.
	switch len(ret) {
	case 0:
		break // ok: no direct output.
	case 1:
		outbound = append(outbound, typex.New(ret[0]))
	case 2:
		outbound = append(outbound, typex.NewKV(typex.New(ret[0]), typex.New(ret[1])))
	default:
		return nil, errors.Errorf("too many return values: %v", ret)
	}

	for _, param := range params {
		values, _ := funcx.UnfoldEmit(param.T)
		trimmed := trimIllegal(values)
		if len(trimmed) == 2 {
			outbound = append(outbound, typex.NewKV(typex.New(trimmed[0]), typex.New(trimmed[1])))
		} else {
			outbound = append(outbound, typex.New(trimmed[0]))
		}
	}
	return outbound, nil
}

func returnTypes(list []funcx.ReturnParam) []reflect.Type {
	var ret []reflect.Type
	for _, elm := range list {
		ret = append(ret, elm.T)
	}
	return ret
}

func findInbound(fn *funcx.Fn, in ...typex.FullType) ([]typex.FullType, []InputKind, error) {
	// log.Printf("Bind inbound: %v %v", fn, in)
	addContext := func(err error, p []funcx.FnParam, in interface{}) error {
		return errors.WithContextf(err, "binding params %v to input %v", p, in)
	}

	var inbound []typex.FullType
	var kinds []InputKind
	params := funcx.SubParams(fn.Param, fn.Params(funcx.FnValue|funcx.FnIter|funcx.FnReIter)...)
	index := 0
	for _, input := range in {
		arity, err := inboundArity(input, index == 0)
		if err != nil {
			return nil, nil, addContext(err, params, input)
		}
		if len(params)-index < arity {
			return nil, nil, addContext(errors.New("too few params"), params[index:], input)
		}

		paramsToBind := params[index : index+arity]
		elm, kind, err := tryBindInbound(input, paramsToBind, index == 0)
		if err != nil {
			return nil, nil, addContext(err, paramsToBind, input)
		}
		inbound = append(inbound, elm)
		kinds = append(kinds, kind)
		index += arity
	}
	if index < len(params) {
		return nil, nil, addContext(errors.New("too few inputs: forgot an input or to annotate options?"), params, in)
	}
	if index > len(params) {
		return nil, nil, addContext(errors.New("too many inputs"), params, in)
	}
	return inbound, kinds, nil
}

func tryBindInbound(t typex.FullType, args []funcx.FnParam, isMain bool) (typex.FullType, InputKind, error) {
	kind := Main
	var other typex.FullType

	switch t.Class() {
	case typex.Concrete, typex.Container:
		if isMain {
			other = typex.New(args[0].T)
		} else {
			// We accept various forms for side input. We have to disambiguate
			// []string into a Singleton of type []string or a Slice of type
			// string by matching up the incoming type and the param type.

			arg := args[0]
			switch arg.Kind {
			case funcx.FnValue:
				if args[0].T.Kind() == reflect.Slice && t.Type() == args[0].T.Elem() {
					// TODO(herohde) 6/29/2017: we do not allow universal slices, for now.

					kind = Slice
					other = typex.New(args[0].T.Elem())
				} else {
					kind = Singleton
					other = typex.New(args[0].T)
				}
			case funcx.FnIter:
				values, _ := funcx.UnfoldIter(args[0].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, errors.Errorf("%v cannot bind to %v", t, args[0])
				}

				kind = Iter
				other = typex.New(trimmed[0])

			case funcx.FnReIter:
				values, _ := funcx.UnfoldReIter(args[0].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, errors.Errorf("%v cannot bind to %v", t, args[0])
				}

				kind = ReIter
				other = typex.New(trimmed[0])

			default:
				return nil, kind, errors.Errorf("unexpected param kind: %v", arg)
			}
		}
	case typex.Composite:
		switch t.Type() {
		case typex.KVType:
			if isMain {
				if args[0].Kind != funcx.FnValue {
					return nil, kind, errors.Errorf("key of %v cannot bind to %v", t, args[0])
				}
				if args[1].Kind != funcx.FnValue {
					return nil, kind, errors.Errorf("value of %v cannot bind to %v", t, args[1])
				}
				other = typex.NewKV(typex.New(args[0].T), typex.New(args[1].T))
			} else {
				// TODO(herohde) 6/29/2017: side input map form.

				switch args[0].Kind {
				case funcx.FnIter:
					values, _ := funcx.UnfoldIter(args[0].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 2 {
						return nil, kind, errors.Errorf("%v cannot bind to %v", t, args[0])
					}

					kind = Iter
					other = typex.NewKV(typex.New(trimmed[0]), typex.New(trimmed[1]))

				case funcx.FnReIter:
					values, _ := funcx.UnfoldReIter(args[0].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 2 {
						return nil, kind, errors.Errorf("%v cannot bind to %v", t, args[0])
					}

					kind = ReIter
					other = typex.NewKV(typex.New(trimmed[0]), typex.New(trimmed[1]))

				default:
					return nil, kind, errors.Errorf("%v cannot bind to %v", t, args[0])
				}
			}

		case typex.CoGBKType:
			if args[0].Kind != funcx.FnValue {
				return nil, kind, errors.Errorf("key of %v cannot bind to %v", t, args[0])
			}

			components := []typex.FullType{typex.New(args[0].T)}

			for i := 1; i < len(args); i++ {
				switch args[i].Kind {
				case funcx.FnIter:
					values, _ := funcx.UnfoldIter(args[i].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 1 {
						return nil, kind, errors.Errorf("values of %v cannot bind to %v", t, args[i])
					}
					components = append(components, typex.New(trimmed[0]))

				case funcx.FnReIter:
					values, _ := funcx.UnfoldReIter(args[i].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 1 {
						return nil, kind, errors.Errorf("values of %v cannot bind to %v", t, args[i])
					}
					components = append(components, typex.New(trimmed[0]))
				default:
					return nil, kind, errors.Errorf("values of %v cannot bind to %v", t, args[i])
				}
			}
			other = typex.NewCoGBK(components...)

		default:
			return nil, kind, errors.Errorf("unexpected inbound type: %v", t.Type())
		}

	default:
		return nil, kind, errors.Errorf("unexpected inbound class: %v", t.Class())
	}

	if !typex.IsStructurallyAssignable(t, other) {
		return nil, kind, errors.Errorf("%v is not assignable to %v", t, other)
	}
	return other, kind, nil
}

func inboundArity(t typex.FullType, isMain bool) (int, error) {
	if t.Class() == typex.Composite {
		switch t.Type() {
		case typex.KVType:
			if isMain {
				return 2, nil
			}
			// A KV side input must be a single iterator/map.
			return 1, nil
		case typex.CoGBKType:
			return len(t.Components()), nil
		default:
			return 0, errors.Errorf("unexpected composite inbound type: %v", t.Type())
		}
	}
	return 1, nil
}

func trimIllegal(list []reflect.Type) []reflect.Type {
	var ret []reflect.Type
	for _, elm := range list {
		switch typex.ClassOf(elm) {
		case typex.Concrete, typex.Universal, typex.Container:
			ret = append(ret, elm)
		}
	}
	return ret
}
