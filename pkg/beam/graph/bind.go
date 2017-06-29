package graph

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
)

// TODO(herohde) 4/21/2017: Bind is where most user mistakes will likely show
// up. We should verify that common mistakes yield reasonable errors.

// Bind returns the inbound, outbound and underlying output types for a UserFn,
// when bound to the underlying input types. All top-level fulltype must be
// Windowed Values, because transforms always work on windowed values for the
// main input and all outputs -- even if the transform chooses to ignore it.
// The complication of bind is primarily that UserFns have loose signatures
// and bind must produce valid type information for the execution plan.
//
// For example,
//
//     func (t EventTime, k typex.X, v int, emit func(string, typex.X))
// or
//     func (context.Context, k typex.X, v int) (string, typex.X, error)
//
// are UserFns that may take one or two incoming fulltypes: either W<KV<X,int>>
// or W<X> with a singleton side input of type W<int>. For the purpose of the
// shape of data processing, the two forms are equivalent. The non-data types,
// context.Context and error, are not part of the data signature, but in play
// only at runtime. EventTime in the first case exposes Window information.
//
// If either was bound to the input type [W<KV<string,int>>], bind would return:
//
//     inbound:  [Main: W<KV<X,int>>]
//     outbound: [W<KV<string,X>>]
//     output:   [W<KV<string,string>>]
//
// Note that it propagates the assignment of X to string in the output type.
//
// If either was instead bound to the input fulltypes [W<float>, W<int>], the
// result would be:
//
//     inbound:  [Main: W<X>, Singleton: W<int>]
//     outbound: [W<KV<string,X>>]
//     output:   [W<KV<string, float>>]
//
// Here, the inbound shape and output types are different from before.
func Bind(fn *userfn.UserFn, in ...typex.FullType) ([]typex.FullType, []InputKind, []typex.FullType, []typex.FullType, error) {
	inbound, kinds, err := findInbound(fn, in...)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	outbound, err := findOutbound(fn)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	subst, err := typex.Bind(inbound, in)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	out, err := typex.Substitute(outbound, subst)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return inbound, kinds, outbound, out, nil
}

func findOutbound(fn *userfn.UserFn) ([]typex.FullType, error) {
	ret := trimIllegal(returnTypes(userfn.SubReturns(fn.Ret, fn.Returns(userfn.RetValue)...)))
	params := userfn.SubParams(fn.Param, fn.Params(userfn.FnEmit)...)

	var outbound []typex.FullType

	// The direct output is the "main" output, if any.
	switch len(ret) {
	case 0:
		break // ok: no direct output.
	case 1:
		outbound = append(outbound, typex.NewW(typex.New(ret[0])))
	case 2:
		outbound = append(outbound, typex.NewWKV(typex.New(ret[0]), typex.New(ret[1])))
	default:
		return nil, fmt.Errorf("too many return values: %v", ret)
	}

	for _, param := range params {
		values, _ := userfn.UnfoldEmit(param.T)
		trimmed := trimIllegal(values)
		if len(trimmed) == 2 {
			outbound = append(outbound, typex.NewWKV(typex.New(trimmed[0]), typex.New(trimmed[1])))
		} else {
			outbound = append(outbound, typex.NewW(typex.New(trimmed[0])))
		}
	}
	return outbound, nil
}

func returnTypes(list []userfn.ReturnParam) []reflect.Type {
	var ret []reflect.Type
	for _, elm := range list {
		ret = append(ret, elm.T)
	}
	return ret
}

func findInbound(fn *userfn.UserFn, in ...typex.FullType) ([]typex.FullType, []InputKind, error) {
	// log.Printf("Bind inbound: %v %v", fn, in)

	var inbound []typex.FullType
	var kinds []InputKind
	params := userfn.SubParams(fn.Param, fn.Params(userfn.FnValue|userfn.FnIter|userfn.FnReIter)...)
	index := 0
	for _, input := range in {
		elm, kind, err := tryBindInbound(input, params[index:], index == 0)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to bind %v to input %v: %v", fn, in, err)
		}
		inbound = append(inbound, elm)
		kinds = append(kinds, kind)
		index += inboundArity(input, index == 0)
	}
	if index < len(params) {
		return nil, nil, fmt.Errorf("found too few input to bind to %v: %v. Forgot an input or to annotate options?", params, in)
	}
	if index > len(params) {
		return nil, nil, fmt.Errorf("found too many input to bind to %v: %v", params, in)
	}
	return inbound, kinds, nil
}

func tryBindInbound(candidate typex.FullType, args []userfn.FnParam, isMain bool) (typex.FullType, InputKind, error) {
	arity := inboundArity(candidate, isMain)
	if len(args) < arity {
		return nil, Main, fmt.Errorf("too few parameters to bind %v", candidate)
	}

	// log.Printf("Bind inbound %v to %v (main: %v)", candidate, args, isMain)

	kind := Main
	var other typex.FullType

	t := typex.SkipW(candidate)
	switch t.Class() {
	case typex.Concrete, typex.Container:
		if isMain {
			other = typex.NewW(typex.New(args[0].T))
		} else {
			// We accept various forms for side input. We have to disambiguate
			// []string into a Singleton of type []string or a Slice of type
			// string by matching up the incoming type and the param type.

			arg := args[0]
			switch arg.Kind {
			case userfn.FnValue:
				if args[0].T.Kind() == reflect.Slice && t.Type() == args[0].T.Elem() {
					// TODO(herohde) 6/29/2017: we do not allow universal slices, for now.

					kind = Slice
					other = typex.NewW(typex.New(args[0].T.Elem()))
				} else {
					kind = Singleton
					other = typex.NewW(typex.New(args[0].T))
				}
			case userfn.FnIter:
				values, _ := userfn.UnfoldIter(args[0].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, fmt.Errorf("%v cannot bind to %v", t, args[0])
				}

				kind = Iter
				other = typex.NewW(typex.New(trimmed[0]))

			case userfn.FnReIter:
				values, _ := userfn.UnfoldReIter(args[0].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, fmt.Errorf("%v cannot bind to %v", t, args[0])
				}

				kind = ReIter
				other = typex.NewW(typex.New(trimmed[0]))

			default:
				panic(fmt.Sprintf("Unexpected param kind: %v", arg))
			}
		}
	case typex.Composite:
		switch t.Type() {
		case typex.KVType:
			if isMain {
				if args[0].Kind != userfn.FnValue {
					return nil, kind, fmt.Errorf("Key of %v cannot bind to %v", t, args[0])
				}
				if args[1].Kind != userfn.FnValue {
					return nil, kind, fmt.Errorf("Value of %v cannot bind to %v", t, args[1])
				}
				other = typex.NewWKV(typex.New(args[0].T), typex.New(args[1].T))
			} else {
				// TODO(herohde) 6/29/2017: side input map form.

				switch args[0].Kind {
				case userfn.FnIter:
					values, _ := userfn.UnfoldIter(args[0].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 2 {
						return nil, kind, fmt.Errorf("%v cannot bind to %v", t, args[0])
					}

					kind = Iter
					other = typex.NewWKV(typex.New(trimmed[0]), typex.New(trimmed[1]))

				case userfn.FnReIter:
					values, _ := userfn.UnfoldReIter(args[0].T)
					trimmed := trimIllegal(values)
					if len(trimmed) != 2 {
						return nil, kind, fmt.Errorf("%v cannot bind to %v", t, args[0])
					}

					kind = ReIter
					other = typex.NewWKV(typex.New(trimmed[0]), typex.New(trimmed[1]))

				default:
					return nil, kind, fmt.Errorf("%v cannot bind to %v", t, args[0])
				}
			}

		case typex.GBKType:
			if args[0].Kind != userfn.FnValue {
				return nil, kind, fmt.Errorf("Key of %v cannot bind to %v", t, args[0])
			}

			switch args[1].Kind {
			case userfn.FnIter:
				values, _ := userfn.UnfoldIter(args[1].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, fmt.Errorf("Values of %v cannot bind to %v", t, args[1])
				}
				other = typex.NewWGBK(typex.New(args[0].T), typex.New(trimmed[0]))

			case userfn.FnReIter:
				values, _ := userfn.UnfoldReIter(args[1].T)
				trimmed := trimIllegal(values)
				if len(trimmed) != 1 {
					return nil, kind, fmt.Errorf("Values of %v cannot bind to %v", t, args[1])
				}
				other = typex.NewWGBK(typex.New(args[0].T), typex.New(trimmed[0]))

			default:
				return nil, kind, fmt.Errorf("Values of %v cannot bind to %v", t, args[1])
			}

		default:
			// TODO: typex.CoGBKType
			panic("Unexpected inbound type")
		}

	default:
		return nil, kind, fmt.Errorf("unexpected inbound type %v", t)
	}

	if !typex.IsStructurallyAssignable(candidate, other) {
		return nil, kind, fmt.Errorf("%v is not assignable to %v", candidate, other)
	}
	return other, kind, nil
}

func inboundArity(t typex.FullType, isMain bool) int {
	if t.Class() == typex.Composite {
		switch t.Type() {
		case typex.KVType:
			if isMain {
				return 2
			}
			// A KV side input must be a single iterator/map.
			return 1
		case typex.GBKType:
			return 2
		case typex.WindowedValueType:
			return inboundArity(t.Components()[0], isMain)
		case typex.CoGBKType:
			return 1 + len(t.Components())
		default:
			panic("Unexpected inbound type")
		}
	}
	return 1
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
