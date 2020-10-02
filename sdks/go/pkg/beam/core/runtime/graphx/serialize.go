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

package graphx

import (
	"fmt"
	"reflect"
	"strings"

	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	v1pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/jsonx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

var genFnType = reflect.TypeOf((*func(string, reflect.Type, []byte) reflectx.Func)(nil)).Elem()

// EncodeMultiEdge converts the preprocessed representation into the wire
// representation of the multiedge, capturing input and output type information.
func EncodeMultiEdge(edge *graph.MultiEdge) (*v1pb.MultiEdge, error) {
	ret := &v1pb.MultiEdge{}
	ret.Opcode = string(edge.Op)

	if edge.DoFn != nil {
		ref, err := encodeFn((*graph.Fn)(edge.DoFn))
		if err != nil {
			wrapped := errors.Wrap(err, "bad userfn")
			return nil, errors.WithContextf(wrapped, "encoding userfn %v", edge)
		}
		ret.Fn = ref
	}
	if edge.CombineFn != nil {
		ref, err := encodeFn((*graph.Fn)(edge.CombineFn))
		if err != nil {
			wrapped := errors.Wrap(err, "bad combinefn")
			return nil, errors.WithContextf(wrapped, "encoding userfn %v", edge)
		}
		ret.Fn = ref
	}
	if edge.WindowFn != nil {
		ret.WindowFn = encodeWindowFn(edge.WindowFn)
	}

	for _, in := range edge.Input {
		kind := encodeInputKind(in.Kind)
		t, err := encodeFullType(in.Type)
		if err != nil {
			wrapped := errors.Wrap(err, "bad input type")
			return nil, errors.WithContextf(wrapped, "encoding userfn %v", edge)
		}
		ret.Inbound = append(ret.Inbound, &v1pb.MultiEdge_Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Output {
		t, err := encodeFullType(out.Type)
		if err != nil {
			wrapped := errors.Wrap(err, "bad output type")
			return nil, errors.WithContextf(wrapped, "encoding userfn %v", edge)
		}
		ret.Outbound = append(ret.Outbound, &v1pb.MultiEdge_Outbound{Type: t})
	}
	return ret, nil
}

// DecodeMultiEdge converts the wire representation into the preprocessed
// components representing that edge. We deserialize to components to avoid
// inserting the edge into a graph or creating a detached edge.
func DecodeMultiEdge(edge *v1pb.MultiEdge) (graph.Opcode, *graph.Fn, *window.Fn, []*graph.Inbound, []*graph.Outbound, error) {
	var u *graph.Fn
	var wfn *window.Fn
	var inbound []*graph.Inbound
	var outbound []*graph.Outbound

	opcode := graph.Opcode(edge.Opcode)

	if edge.Fn != nil {
		var err error
		u, err = decodeFn(edge.Fn)
		if err != nil {
			wrapped := errors.Wrap(err, "bad function")
			return "", nil, nil, nil, nil, errors.WithContextf(wrapped, "decoding userfn %v", edge)
		}
	}
	if edge.WindowFn != nil {
		wfn = decodeWindowFn(edge.WindowFn)
	}
	for _, in := range edge.Inbound {
		kind, err := decodeInputKind(in.Kind)
		if err != nil {
			wrapped := errors.Wrap(err, "bad input kind")
			return "", nil, nil, nil, nil, errors.WithContextf(wrapped, "decoding userfn %v", edge)
		}
		t, err := decodeFullType(in.Type)
		if err != nil {
			wrapped := errors.Wrap(err, "bad input type")
			return "", nil, nil, nil, nil, errors.WithContextf(wrapped, "decoding userfn %v", edge)
		}
		inbound = append(inbound, &graph.Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Outbound {
		t, err := decodeFullType(out.Type)
		if err != nil {
			wrapped := errors.Wrap(err, "bad output type")
			return "", nil, nil, nil, nil, errors.WithContextf(wrapped, "decoding userfn %v", edge)
		}
		outbound = append(outbound, &graph.Outbound{Type: t})
	}

	return opcode, u, wfn, inbound, outbound, nil
}

func encodeCustomCoder(c *coder.CustomCoder) (*v1pb.CustomCoder, error) {
	t, err := encodeType(c.Type)
	if err != nil {
		return nil, errors.WithContextf(err, "encoding custom coder %v for type %v", c, c.Type)
	}
	enc, err := encodeUserFn(c.Enc)
	if err != nil {
		wrapped := errors.Wrap(err, "bad encoding function")
		return nil, errors.WithContextf(wrapped, "encoding custom coder %v", c)
	}
	dec, err := encodeUserFn(c.Dec)
	if err != nil {
		wrapped := errors.Wrap(err, "bad decoding function")
		return nil, errors.WithContextf(wrapped, "encoding custom coder %v", c)
	}

	ret := &v1pb.CustomCoder{
		Name: c.Name,
		Type: t,
		Enc:  enc,
		Dec:  dec,
	}
	return ret, nil
}

func decodeCustomCoder(c *v1pb.CustomCoder) (*coder.CustomCoder, error) {
	t, err := decodeType(c.Type)
	if err != nil {
		return nil, errors.WithContextf(err, "decoding custom coder %v for type %v", c, c.Type)
	}
	enc, err := decodeUserFn(c.Enc)
	if err != nil {
		wrapped := errors.Wrap(err, "bad encoding function")
		return nil, errors.WithContextf(wrapped, "decoding custom coder %v", c)
	}
	dec, err := decodeUserFn(c.Dec)
	if err != nil {
		wrapped := errors.Wrap(err, "bad decoding function")
		return nil, errors.WithContextf(wrapped, "decoding custom coder %v", c)
	}

	ret, err := coder.NewCustomCoder(c.Name, t, enc, dec)
	if err != nil {
		return nil, errors.WithContextf(err, "decoding custom coder %v", c)
	}
	return ret, nil
}

func encodeWindowFn(w *window.Fn) *v1pb.WindowFn {
	return &v1pb.WindowFn{
		Kind:     string(w.Kind),
		SizeMs:   duration2ms(w.Size),
		PeriodMs: duration2ms(w.Period),
		GapMs:    duration2ms(w.Gap),
	}
}

func decodeWindowFn(w *v1pb.WindowFn) *window.Fn {
	return &window.Fn{
		Kind:   window.Kind(w.Kind),
		Size:   ms2duration(w.SizeMs),
		Period: ms2duration(w.PeriodMs),
		Gap:    ms2duration(w.GapMs),
	}
}

func duration2ms(d time.Duration) int64 {
	return d.Nanoseconds() / 1e6
}

func ms2duration(d int64) time.Duration {
	return time.Duration(d) * time.Millisecond
}

func encodeFn(u *graph.Fn) (*v1pb.Fn, error) {
	switch {
	case u.DynFn != nil:
		gen := reflectx.FunctionName(u.DynFn.Gen)
		t, err := encodeType(u.DynFn.T)
		if err != nil {
			wrapped := errors.Wrap(err, "bad function type")
			return nil, errors.WithContextf(wrapped, "encoding dynamic DoFn %v", u)
		}
		return &v1pb.Fn{Dynfn: &v1pb.DynFn{
			Name: u.DynFn.Name,
			Type: t,
			Data: u.DynFn.Data,
			Gen:  gen,
		}}, nil

	case u.Fn != nil:
		fn, err := encodeUserFn(u.Fn)
		if err != nil {
			wrapped := errors.Wrap(err, "bad userfn")
			return nil, errors.WithContextf(wrapped, "encoding DoFn %v", u)
		}
		return &v1pb.Fn{Fn: fn}, nil

	case u.Recv != nil:
		t := reflect.TypeOf(u.Recv)
		k, ok := runtime.TypeKey(reflectx.SkipPtr(t))
		if !ok {
			err := errors.Errorf("failed to create TypeKey for receiver type %T", u.Recv)
			return nil, errors.WithContextf(err, "encoding structural DoFn %v", u)
		}
		if _, ok := runtime.LookupType(k); !ok {
			err := errors.Errorf("receiver type %v must be registered", t)
			return nil, errors.WithContextf(err, "encoding structural DoFn %v", u)
		}
		typ, err := encodeType(t)
		if err != nil {
			wrapped := errors.Wrapf(err, "failed to encode receiver type %T", u.Recv)
			panic(errors.WithContextf(wrapped, "encoding structural DoFn %v", u))
		}

		data, err := jsonx.Marshal(u.Recv)
		if err != nil {
			wrapped := errors.Wrapf(err, "failed to marshal receiver %v", u.Recv)
			return nil, errors.WithContextf(wrapped, "encoding structural DoFn %v", u)
		}
		return &v1pb.Fn{Type: typ, Opt: string(data)}, nil

	default:
		panic(fmt.Sprintf("Failed to encode DoFn %v, missing fn", u))
	}
}

func decodeFn(u *v1pb.Fn) (*graph.Fn, error) {
	if u.Dynfn != nil {
		gen, err := runtime.ResolveFunction(u.Dynfn.Gen, genFnType)
		if err != nil {
			wrapped := errors.Wrapf(err, "bad symbol %v", u.Dynfn.Gen)
			return nil, errors.WithContextf(wrapped, "decoding dynamic DoFn %v", u)
		}

		t, err := decodeType(u.Dynfn.Type)
		if err != nil {
			wrapped := errors.Wrap(err, "bad type")
			return nil, errors.WithContextf(wrapped, "failed to decode dynamic DoFn %v", u)
		}
		return graph.NewFn(&graph.DynFn{
			Name: u.Dynfn.Name,
			T:    t,
			Data: u.Dynfn.Data,
			Gen:  gen.(func(string, reflect.Type, []byte) reflectx.Func),
		})
	}
	if u.Fn != nil {
		fn, err := decodeUserFn(u.Fn)
		if err != nil {
			wrapped := errors.Wrap(err, "failed to decode userfn")
			return nil, errors.WithContextf(wrapped, "decoding DoFn %v", u)
		}
		fx, err := funcx.New(reflectx.MakeFunc(fn))
		if err != nil {
			wrapped := errors.Wrap(err, "failed to construct userfn")
			return nil, errors.WithContextf(wrapped, "decoding DoFn %v", u)
		}
		return &graph.Fn{Fn: fx}, nil
	}

	t, err := decodeType(u.Type)
	if err != nil {
		wrapped := errors.Wrap(err, "bad type")
		return nil, errors.WithContextf(wrapped, "decoding structural DoFn %v", u)
	}
	elem := reflect.New(t)
	if err := jsonx.UnmarshalFrom(elem.Interface(), strings.NewReader(u.Opt)); err != nil {
		wrapped := errors.Wrap(err, "bad struct encoding")
		return nil, errors.WithContextf(wrapped, "decoding structural DoFn %v", u)
	}
	fn := elem.Elem().Interface()
	return graph.NewFn(fn)
}

// encodeUserFn translates the preprocessed representation of a Beam user function
// into the wire representation, capturing all the inputs and outputs needed.
func encodeUserFn(u *funcx.Fn) (*v1pb.UserFn, error) {
	// TODO(herohde) 5/23/2017: reject closures and dynamic functions. They can't
	// be serialized.

	symbol := u.Fn.Name()
	t, err := encodeType(u.Fn.Type())
	if err != nil {
		wrapped := errors.Wrap(err, "bad function type")
		return nil, errors.WithContextf(wrapped, "encoding userfn %v", u)
	}
	return &v1pb.UserFn{Name: symbol, Type: t}, nil
}

// decodeUserFn receives the wire representation of a Beam user function,
// extracting the preprocessed representation, expanding all inputs and outputs
// of the function.
func decodeUserFn(ref *v1pb.UserFn) (interface{}, error) {
	t, err := decodeType(ref.GetType())
	if err != nil {
		return nil, err
	}

	return runtime.ResolveFunction(ref.Name, t)
}

func encodeFullType(t typex.FullType) (*v1pb.FullType, error) {
	var components []*v1pb.FullType
	if t.Class() == typex.Composite {
		// Drop the Aggregate convenience component.

		for _, comp := range t.Components() {
			c, err := encodeFullType(comp)
			if err != nil {
				return nil, err
			}
			components = append(components, c)
		}
	}

	prim, err := encodeType(t.Type())
	if err != nil {
		wrapped := errors.Wrap(err, "bad type")
		return nil, errors.WithContextf(wrapped, "encoding full type %v", t)
	}
	return &v1pb.FullType{Type: prim, Components: components}, nil
}

func decodeFullType(t *v1pb.FullType) (typex.FullType, error) {
	var components []typex.FullType
	for _, comp := range t.Components {
		c, err := decodeFullType(comp)
		if err != nil {
			return nil, err
		}
		components = append(components, c)
	}

	prim, err := decodeType(t.Type)
	if err != nil {
		wrapped := errors.Wrap(err, "bad type")
		return nil, errors.WithContextf(wrapped, "decoding full type %v", t)
	}
	return typex.New(prim, components...), nil
}

func encodeType(t reflect.Type) (*v1pb.Type, error) {
	if s, ok := tryEncodeSpecial(t); ok {
		return &v1pb.Type{Kind: v1pb.Type_SPECIAL, Special: s}, nil
	}
	if k, ok := runtime.TypeKey(t); ok {
		if _, present := runtime.LookupType(k); present {
			// External type. Serialize by key and lookup in registry
			// on decoding side.

			return &v1pb.Type{Kind: v1pb.Type_EXTERNAL, ExternalKey: k}, nil
		}
	}

	// The supplied type isn't special, so apply the standard encodings.
	switch t.Kind() {
	case reflect.Bool:
		return &v1pb.Type{Kind: v1pb.Type_BOOL}, nil
	case reflect.Int:
		return &v1pb.Type{Kind: v1pb.Type_INT}, nil
	case reflect.Int8:
		return &v1pb.Type{Kind: v1pb.Type_INT8}, nil
	case reflect.Int16:
		return &v1pb.Type{Kind: v1pb.Type_INT16}, nil
	case reflect.Int32:
		return &v1pb.Type{Kind: v1pb.Type_INT32}, nil
	case reflect.Int64:
		return &v1pb.Type{Kind: v1pb.Type_INT64}, nil
	case reflect.Uint:
		return &v1pb.Type{Kind: v1pb.Type_UINT}, nil
	case reflect.Uint8:
		return &v1pb.Type{Kind: v1pb.Type_UINT8}, nil
	case reflect.Uint16:
		return &v1pb.Type{Kind: v1pb.Type_UINT16}, nil
	case reflect.Uint32:
		return &v1pb.Type{Kind: v1pb.Type_UINT32}, nil
	case reflect.Uint64:
		return &v1pb.Type{Kind: v1pb.Type_UINT64}, nil
	case reflect.Float32:
		return &v1pb.Type{Kind: v1pb.Type_FLOAT32}, nil
	case reflect.Float64:
		return &v1pb.Type{Kind: v1pb.Type_FLOAT64}, nil
	case reflect.String:
		return &v1pb.Type{Kind: v1pb.Type_STRING}, nil

	case reflect.Slice:
		elm, err := encodeType(t.Elem())
		if err != nil {
			wrapped := errors.Wrap(err, "bad element type")
			return nil, errors.WithContextf(wrapped, "encoding slice %v", t)
		}
		return &v1pb.Type{Kind: v1pb.Type_SLICE, Element: elm}, nil

	case reflect.Struct:
		var fields []*v1pb.Type_StructField
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			if f.PkgPath != "" {
				wrapped := errors.Errorf("type has unexported field: %v", f.Name)
				return nil, errors.WithContextf(wrapped, "encoding struct %v", t)
			}

			fType, err := encodeType(f.Type)
			if err != nil {
				wrapped := errors.Wrap(err, "bad field type")
				return nil, errors.WithContextf(wrapped, "encoding struct %v", t)
			}

			field := &v1pb.Type_StructField{
				Name:      f.Name,
				PkgPath:   f.PkgPath,
				Type:      fType,
				Tag:       string(f.Tag),
				Offset:    int64(f.Offset),
				Index:     encodeInts(f.Index),
				Anonymous: f.Anonymous,
			}
			fields = append(fields, field)
		}
		return &v1pb.Type{Kind: v1pb.Type_STRUCT, Fields: fields}, nil

	case reflect.Func:
		var in []*v1pb.Type
		for i := 0; i < t.NumIn(); i++ {
			param, err := encodeType(t.In(i))
			if err != nil {
				wrapped := errors.Wrap(err, "bad parameter type")
				return nil, errors.WithContextf(wrapped, "encoding function %v", t)
			}
			in = append(in, param)
		}
		var out []*v1pb.Type
		for i := 0; i < t.NumOut(); i++ {
			ret, err := encodeType(t.Out(i))
			if err != nil {
				wrapped := errors.Wrap(err, "bad return type")
				return nil, errors.WithContextf(wrapped, "encoding function %v", t)
			}
			out = append(out, ret)
		}
		return &v1pb.Type{Kind: v1pb.Type_FUNC, ParameterTypes: in, ReturnTypes: out, IsVariadic: t.IsVariadic()}, nil

	case reflect.Chan:
		elm, err := encodeType(t.Elem())
		if err != nil {
			wrapped := errors.Wrap(err, "bad element type")
			return nil, errors.WithContextf(wrapped, "encoding channel %v", t)
		}
		dir := encodeChanDir(t.ChanDir())
		return &v1pb.Type{Kind: v1pb.Type_CHAN, Element: elm, ChanDir: dir}, nil

	case reflect.Ptr:
		elm, err := encodeType(t.Elem())
		if err != nil {
			wrapped := errors.Wrap(err, "bad base type")
			return nil, errors.WithContextf(wrapped, "encoding pointer %v", t)
		}
		return &v1pb.Type{Kind: v1pb.Type_PTR, Element: elm}, nil

	default:
		return nil, errors.Errorf("unencodable type %v", t)
	}
}

func tryEncodeSpecial(t reflect.Type) (v1pb.Type_Special, bool) {
	switch t {
	case reflectx.Error:
		return v1pb.Type_ERROR, true
	case reflectx.Context:
		return v1pb.Type_CONTEXT, true
	case reflectx.Type:
		return v1pb.Type_TYPE, true

	case typex.EventTimeType:
		return v1pb.Type_EVENTTIME, true
	case typex.WindowType:
		return v1pb.Type_WINDOW, true
	case typex.KVType:
		return v1pb.Type_KV, true
	case typex.CoGBKType:
		return v1pb.Type_COGBK, true
	case typex.WindowedValueType:
		return v1pb.Type_WINDOWEDVALUE, true

	case typex.TType:
		return v1pb.Type_T, true
	case typex.UType:
		return v1pb.Type_U, true
	case typex.VType:
		return v1pb.Type_V, true
	case typex.WType:
		return v1pb.Type_W, true
	case typex.XType:
		return v1pb.Type_X, true
	case typex.YType:
		return v1pb.Type_Y, true
	case typex.ZType:
		return v1pb.Type_Z, true

	default:
		return v1pb.Type_ILLEGAL, false
	}
}

func decodeType(t *v1pb.Type) (reflect.Type, error) {
	if t == nil {
		err := errors.New("empty type")
		return nil, errors.WithContextf(err, "decoding type %v", t)
	}

	switch t.Kind {
	case v1pb.Type_BOOL:
		return reflectx.Bool, nil
	case v1pb.Type_INT:
		return reflectx.Int, nil
	case v1pb.Type_INT8:
		return reflectx.Int8, nil
	case v1pb.Type_INT16:
		return reflectx.Int16, nil
	case v1pb.Type_INT32:
		return reflectx.Int32, nil
	case v1pb.Type_INT64:
		return reflectx.Int64, nil
	case v1pb.Type_UINT:
		return reflectx.Uint, nil
	case v1pb.Type_UINT8:
		return reflectx.Uint8, nil
	case v1pb.Type_UINT16:
		return reflectx.Uint16, nil
	case v1pb.Type_UINT32:
		return reflectx.Uint32, nil
	case v1pb.Type_UINT64:
		return reflectx.Uint64, nil
	case v1pb.Type_FLOAT32:
		return reflectx.Float32, nil
	case v1pb.Type_FLOAT64:
		return reflectx.Float64, nil
	case v1pb.Type_STRING:
		return reflectx.String, nil

	case v1pb.Type_SLICE:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			wrapped := errors.Wrap(err, "bad element")
			return nil, errors.WithContextf(wrapped, "failed to decode type %v, bad element", t)
		}
		return reflect.SliceOf(elm), nil

	case v1pb.Type_STRUCT:
		var fields []reflect.StructField
		for _, f := range t.Fields {
			fType, err := decodeType(f.Type)
			if err != nil {
				wrapped := errors.Wrap(err, "bad field type")
				return nil, errors.WithContextf(wrapped, "failed to decode type %v, bad field type", t)
			}

			field := reflect.StructField{
				Name:      f.GetName(),
				PkgPath:   f.GetPkgPath(),
				Type:      fType,
				Tag:       reflect.StructTag(f.GetTag()),
				Offset:    uintptr(f.GetOffset()),
				Index:     decodeInts(f.GetIndex()),
				Anonymous: f.GetAnonymous(),
			}
			fields = append(fields, field)
		}
		return reflect.StructOf(fields), nil

	case v1pb.Type_FUNC:
		in, err := decodeTypes(t.GetParameterTypes())
		if err != nil {
			wrapped := errors.Wrap(err, "bad parameter type")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		out, err := decodeTypes(t.GetReturnTypes())
		if err != nil {
			wrapped := errors.Wrap(err, "bad return type")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		return reflect.FuncOf(in, out, t.GetIsVariadic()), nil

	case v1pb.Type_CHAN:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			wrapped := errors.Wrap(err, "bad element")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		dir, err := decodeChanDir(t.GetChanDir())
		if err != nil {
			wrapped := errors.Wrap(err, "bad channel direction")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		return reflect.ChanOf(dir, elm), nil

	case v1pb.Type_PTR:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			wrapped := errors.Wrap(err, "bad element")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		return reflect.PtrTo(elm), nil

	case v1pb.Type_SPECIAL:
		ret, err := decodeSpecial(t.Special)
		if err != nil {
			wrapped := errors.Wrap(err, "bad element")
			return nil, errors.WithContextf(wrapped, "decoding type %v", t)
		}
		return ret, nil

	case v1pb.Type_EXTERNAL:
		ret, ok := runtime.LookupType(t.ExternalKey)
		if !ok {
			err := errors.Errorf("external key not found %v", t.ExternalKey)
			return nil, errors.WithContextf(err, "decoding type %v", t)
		}
		return ret, nil

	default:
		err := errors.Errorf("unexpected type kind %v", t.Kind)
		return nil, errors.WithContextf(err, "failed to decode type %v", t)
	}
}

func decodeSpecial(s v1pb.Type_Special) (reflect.Type, error) {
	switch s {
	case v1pb.Type_ERROR:
		return reflectx.Error, nil
	case v1pb.Type_CONTEXT:
		return reflectx.Context, nil
	case v1pb.Type_TYPE:
		return reflectx.Type, nil

	case v1pb.Type_EVENTTIME:
		return typex.EventTimeType, nil
	case v1pb.Type_WINDOW:
		return typex.WindowType, nil
	case v1pb.Type_KV:
		return typex.KVType, nil
	case v1pb.Type_COGBK:
		return typex.CoGBKType, nil
	case v1pb.Type_WINDOWEDVALUE:
		return typex.WindowedValueType, nil

	case v1pb.Type_T:
		return typex.TType, nil
	case v1pb.Type_U:
		return typex.UType, nil
	case v1pb.Type_V:
		return typex.VType, nil
	case v1pb.Type_W:
		return typex.WType, nil
	case v1pb.Type_X:
		return typex.XType, nil
	case v1pb.Type_Y:
		return typex.YType, nil
	case v1pb.Type_Z:
		return typex.ZType, nil

	default:
		return nil, errors.Errorf("failed to decode special type, unknown type %v", s)
	}
}

func decodeTypes(list []*v1pb.Type) ([]reflect.Type, error) {
	var ret []reflect.Type
	for _, elm := range list {
		t, err := decodeType(elm)
		if err != nil {
			return nil, err
		}
		ret = append(ret, t)
	}
	return ret, nil
}

func encodeInts(offsets []int) []int32 {
	var ret []int32
	for _, elm := range offsets {
		ret = append(ret, int32(elm))
	}
	return ret
}

func decodeInts(offsets []int32) []int {
	var ret []int
	for _, elm := range offsets {
		ret = append(ret, int(elm))
	}
	return ret
}

func encodeChanDir(dir reflect.ChanDir) v1pb.Type_ChanDir {
	switch dir {
	case reflect.RecvDir:
		return v1pb.Type_RECV
	case reflect.SendDir:
		return v1pb.Type_SEND
	case reflect.BothDir:
		return v1pb.Type_BOTH
	default:
		panic(fmt.Sprintf("Failed to encode channel direction, invalid value: %v", dir))
	}
}

func decodeChanDir(dir v1pb.Type_ChanDir) (reflect.ChanDir, error) {
	switch dir {
	case v1pb.Type_RECV:
		return reflect.RecvDir, nil
	case v1pb.Type_SEND:
		return reflect.SendDir, nil
	case v1pb.Type_BOTH:
		return reflect.BothDir, nil
	default:
		err := errors.Errorf("invalid value: %v", dir)
		return reflect.BothDir, errors.WithContext(err, "decoding channel direction")
	}
}

func encodeInputKind(k graph.InputKind) v1pb.MultiEdge_Inbound_InputKind {
	switch k {
	case graph.Main:
		return v1pb.MultiEdge_Inbound_MAIN
	case graph.Singleton:
		return v1pb.MultiEdge_Inbound_SINGLETON
	case graph.Slice:
		return v1pb.MultiEdge_Inbound_SLICE
	case graph.Map:
		return v1pb.MultiEdge_Inbound_MAP
	case graph.MultiMap:
		return v1pb.MultiEdge_Inbound_MULTIMAP
	case graph.Iter:
		return v1pb.MultiEdge_Inbound_ITER
	case graph.ReIter:
		return v1pb.MultiEdge_Inbound_REITER
	default:
		panic(fmt.Sprintf("Failed to encode input kind, invalid value: %v", k))
	}
}

func decodeInputKind(k v1pb.MultiEdge_Inbound_InputKind) (graph.InputKind, error) {
	switch k {
	case v1pb.MultiEdge_Inbound_MAIN:
		return graph.Main, nil
	case v1pb.MultiEdge_Inbound_SINGLETON:
		return graph.Singleton, nil
	case v1pb.MultiEdge_Inbound_SLICE:
		return graph.Slice, nil
	case v1pb.MultiEdge_Inbound_MAP:
		return graph.Map, nil
	case v1pb.MultiEdge_Inbound_MULTIMAP:
		return graph.MultiMap, nil
	case v1pb.MultiEdge_Inbound_ITER:
		return graph.Iter, nil
	case v1pb.MultiEdge_Inbound_REITER:
		return graph.ReIter, nil
	default:
		err := errors.Errorf("invalid value: %v", k)
		return graph.Main, errors.WithContext(err, "decoding input kind")
	}
}
