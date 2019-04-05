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
	"encoding/json"
	"fmt"
	"reflect"

	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var genFnType = reflect.TypeOf((*func(string, reflect.Type, []byte) reflectx.Func)(nil)).Elem()

// EncodeMultiEdge converts the preprocessed representation into the wire
// representation of the multiedge, capturing input and output type information.
func EncodeMultiEdge(edge *graph.MultiEdge) (*v1.MultiEdge, error) {
	ret := &v1.MultiEdge{}
	ret.Opcode = string(edge.Op)

	if edge.DoFn != nil {
		ref, err := encodeFn((*graph.Fn)(edge.DoFn))
		if err != nil {
			return nil, fmt.Errorf("failed to encode userfn %v, bad userfn: %v", edge, err)
		}
		ret.Fn = ref
	}
	if edge.CombineFn != nil {
		ref, err := encodeFn((*graph.Fn)(edge.CombineFn))
		if err != nil {
			return nil, fmt.Errorf("failed to encode userfn %v, bad combinefn: %v", edge, err)
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
			return nil, fmt.Errorf("failed to encode userfn %v, bad input type: %v", edge, err)
		}
		ret.Inbound = append(ret.Inbound, &v1.MultiEdge_Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Output {
		t, err := encodeFullType(out.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to encode userfn %v, bad output type: %v", edge, err)
		}
		ret.Outbound = append(ret.Outbound, &v1.MultiEdge_Outbound{Type: t})
	}
	return ret, nil
}

// DecodeMultiEdge converts the wire representation into the preprocessed
// components representing that edge. We deserialize to components to avoid
// inserting the edge into a graph or creating a detached edge.
func DecodeMultiEdge(edge *v1.MultiEdge) (graph.Opcode, *graph.Fn, *window.Fn, []*graph.Inbound, []*graph.Outbound, error) {
	var u *graph.Fn
	var wfn *window.Fn
	var inbound []*graph.Inbound
	var outbound []*graph.Outbound

	opcode := graph.Opcode(edge.Opcode)

	if edge.Fn != nil {
		var err error
		u, err = decodeFn(edge.Fn)
		if err != nil {
			return "", nil, nil, nil, nil, fmt.Errorf("failed to decode userfn %v, bad function: %v", edge, err)
		}
	}
	if edge.WindowFn != nil {
		wfn = decodeWindowFn(edge.WindowFn)
	}
	for _, in := range edge.Inbound {
		kind, err := decodeInputKind(in.Kind)
		if err != nil {
			return "", nil, nil, nil, nil, fmt.Errorf("failed to decode userfn %v, bad input kind: %v", edge, err)
		}
		t, err := decodeFullType(in.Type)
		if err != nil {
			return "", nil, nil, nil, nil, fmt.Errorf("failed to decode userfn %v, bad input type: %v", edge, err)
		}
		inbound = append(inbound, &graph.Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Outbound {
		t, err := decodeFullType(out.Type)
		if err != nil {
			return "", nil, nil, nil, nil, fmt.Errorf("failed to decode userfn %v, bad output type: %v", edge, err)
		}
		outbound = append(outbound, &graph.Outbound{Type: t})
	}

	return opcode, u, wfn, inbound, outbound, nil
}

func encodeCustomCoder(c *coder.CustomCoder) (*v1.CustomCoder, error) {
	t, err := encodeType(c.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to encode custom coder %v for type %v: %v", c, c.Type, err)
	}
	enc, err := encodeUserFn(c.Enc)
	if err != nil {
		return nil, fmt.Errorf("failed to encode custom coder %v, bad encoding function: %v", c, err)
	}
	dec, err := encodeUserFn(c.Dec)
	if err != nil {
		return nil, fmt.Errorf("failed to encode custom coder %v, bad decoding function: %v", c, err)
	}

	ret := &v1.CustomCoder{
		Name: c.Name,
		Type: t,
		Enc:  enc,
		Dec:  dec,
	}
	return ret, nil
}

func decodeCustomCoder(c *v1.CustomCoder) (*coder.CustomCoder, error) {
	t, err := decodeType(c.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to decode custom coder %v for type %v: %v", c, c.Type, err)
	}
	enc, err := decodeUserFn(c.Enc)
	if err != nil {
		return nil, fmt.Errorf("failed to decode custom coder %v, bad encoding function: %v", c, err)
	}
	dec, err := decodeUserFn(c.Dec)
	if err != nil {
		return nil, fmt.Errorf("failed to decode custom coder %v, bad decoding function: %v", c, err)
	}

	ret, err := coder.NewCustomCoder(c.Name, t, enc, dec)
	if err != nil {
		return nil, fmt.Errorf("failed to decode custom coder %v: %v", c, err)
	}
	return ret, nil
}

func encodeWindowFn(w *window.Fn) *v1.WindowFn {
	return &v1.WindowFn{
		Kind:     string(w.Kind),
		SizeMs:   duration2ms(w.Size),
		PeriodMs: duration2ms(w.Period),
		GapMs:    duration2ms(w.Gap),
	}
}

func decodeWindowFn(w *v1.WindowFn) *window.Fn {
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

func encodeFn(u *graph.Fn) (*v1.Fn, error) {
	switch {
	case u.DynFn != nil:
		gen := reflectx.FunctionName(u.DynFn.Gen)
		t, err := encodeType(u.DynFn.T)
		if err != nil {
			return nil, fmt.Errorf("failed to encode dynamic DoFn %v, bad function type: %v", u, err)
		}
		return &v1.Fn{Dynfn: &v1.DynFn{
			Name: u.DynFn.Name,
			Type: t,
			Data: u.DynFn.Data,
			Gen:  gen,
		}}, nil

	case u.Fn != nil:
		fn, err := encodeUserFn(u.Fn)
		if err != nil {
			return nil, fmt.Errorf("failed to encode DoFn %v, bad userfn: %v", u, err)
		}
		return &v1.Fn{Fn: fn}, nil

	case u.Recv != nil:
		t := reflect.TypeOf(u.Recv)
		k, ok := runtime.TypeKey(reflectx.SkipPtr(t))
		if !ok {
			return nil, fmt.Errorf("failed to encode structural DoFn %v, failed to create TypeKey for receiver type %T", u, u.Recv)
		}
		if _, ok := runtime.LookupType(k); !ok {
			return nil, fmt.Errorf("failed to encode structural DoFn %v, receiver type %v must be registered", u, t)
		}
		typ, err := encodeType(t)
		if err != nil {
			panic(fmt.Sprintf("Failed to encode structural DoFn %v, failed to encode receiver type %T: %v", u, u.Recv, err))
		}

		data, err := json.Marshal(u.Recv)
		if err != nil {
			return nil, fmt.Errorf("failed to encode structural DoFn %v, failed to marshal receiver %v: %v", u, u.Recv, err)
		}
		return &v1.Fn{Type: typ, Opt: string(data)}, nil

	default:
		panic(fmt.Sprintf("Failed to encode DoFn %v, missing fn", u))
	}
}

func decodeFn(u *v1.Fn) (*graph.Fn, error) {
	if u.Dynfn != nil {
		gen, err := runtime.ResolveFunction(u.Dynfn.Gen, genFnType)
		if err != nil {
			return nil, fmt.Errorf("failed to decode dynamic DoFn %v, bad symbol %v: %v", u, u.Dynfn.Gen, err)
		}

		t, err := decodeType(u.Dynfn.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to decode dynamic DoFn %v, bad type: %v", u, err)
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
			return nil, fmt.Errorf("failed to decode DoFn %v, failed to decode userfn: %v", u, err)
		}
		fx, err := funcx.New(reflectx.MakeFunc(fn))
		if err != nil {
			return nil, fmt.Errorf("failed to decode DoFn %v, failed to construct userfn: %v", u, err)
		}
		return &graph.Fn{Fn: fx}, nil
	}

	t, err := decodeType(u.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to decode structural DoFn %v, bad type: %v", u, err)
	}
	fn, err := reflectx.UnmarshalJSON(t, u.Opt)
	if err != nil {
		return nil, fmt.Errorf("failed to decode structural DoFn %v, bad struct encoding: %v", u, err)
	}
	return graph.NewFn(fn)
}

// encodeUserFn translates the preprocessed representation of a Beam user function
// into the wire representation, capturing all the inputs and outputs needed.
func encodeUserFn(u *funcx.Fn) (*v1.UserFn, error) {
	// TODO(herohde) 5/23/2017: reject closures and dynamic functions. They can't
	// be serialized.

	symbol := u.Fn.Name()
	t, err := encodeType(u.Fn.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to encode userfn %v, bad function type: %v", u, err)
	}
	return &v1.UserFn{Name: symbol, Type: t}, nil
}

// decodeUserFn receives the wire representation of a Beam user function,
// extracting the preprocessed representation, expanding all inputs and outputs
// of the function.
func decodeUserFn(ref *v1.UserFn) (interface{}, error) {
	t, err := decodeType(ref.GetType())
	if err != nil {
		return nil, err
	}

	return runtime.ResolveFunction(ref.Name, t)
}

func encodeFullType(t typex.FullType) (*v1.FullType, error) {
	var components []*v1.FullType
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
		return nil, fmt.Errorf("failed to encode full type %v, bad type: %v", t, err)
	}
	return &v1.FullType{Type: prim, Components: components}, nil
}

func decodeFullType(t *v1.FullType) (typex.FullType, error) {
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
		return nil, fmt.Errorf("failed to decode full type %v, bad type: %v", t, err)
	}
	return typex.New(prim, components...), nil
}

func encodeType(t reflect.Type) (*v1.Type, error) {
	if s, ok := tryEncodeSpecial(t); ok {
		return &v1.Type{Kind: v1.Type_SPECIAL, Special: s}, nil
	}
	if k, ok := runtime.TypeKey(t); ok {
		if _, present := runtime.LookupType(k); present {
			// External type. Serialize by key and lookup in registry
			// on decoding side.

			return &v1.Type{Kind: v1.Type_EXTERNAL, ExternalKey: k}, nil
		}
	}

	// The supplied type isn't special, so apply the standard encodings.
	switch t.Kind() {
	case reflect.Bool:
		return &v1.Type{Kind: v1.Type_BOOL}, nil
	case reflect.Int:
		return &v1.Type{Kind: v1.Type_INT}, nil
	case reflect.Int8:
		return &v1.Type{Kind: v1.Type_INT8}, nil
	case reflect.Int16:
		return &v1.Type{Kind: v1.Type_INT16}, nil
	case reflect.Int32:
		return &v1.Type{Kind: v1.Type_INT32}, nil
	case reflect.Int64:
		return &v1.Type{Kind: v1.Type_INT64}, nil
	case reflect.Uint:
		return &v1.Type{Kind: v1.Type_UINT}, nil
	case reflect.Uint8:
		return &v1.Type{Kind: v1.Type_UINT8}, nil
	case reflect.Uint16:
		return &v1.Type{Kind: v1.Type_UINT16}, nil
	case reflect.Uint32:
		return &v1.Type{Kind: v1.Type_UINT32}, nil
	case reflect.Uint64:
		return &v1.Type{Kind: v1.Type_UINT64}, nil
	case reflect.Float32:
		return &v1.Type{Kind: v1.Type_FLOAT32}, nil
	case reflect.Float64:
		return &v1.Type{Kind: v1.Type_FLOAT64}, nil
	case reflect.String:
		return &v1.Type{Kind: v1.Type_STRING}, nil

	case reflect.Slice:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("failed to encode slice %v, bad element type: %v", t, err)
		}
		return &v1.Type{Kind: v1.Type_SLICE, Element: elm}, nil

	case reflect.Struct:
		var fields []*v1.Type_StructField
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			fType, err := encodeType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to encode struct %v, bad field type: %v", t, err)
			}

			field := &v1.Type_StructField{
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
		return &v1.Type{Kind: v1.Type_STRUCT, Fields: fields}, nil

	case reflect.Func:
		var in []*v1.Type
		for i := 0; i < t.NumIn(); i++ {
			param, err := encodeType(t.In(i))
			if err != nil {
				return nil, fmt.Errorf("failed to encode function %v, bad parameter type: %v", t, err)
			}
			in = append(in, param)
		}
		var out []*v1.Type
		for i := 0; i < t.NumOut(); i++ {
			ret, err := encodeType(t.Out(i))
			if err != nil {
				return nil, fmt.Errorf("failed to encode function %v, bad return type: %v", t, err)
			}
			out = append(out, ret)
		}
		return &v1.Type{Kind: v1.Type_FUNC, ParameterTypes: in, ReturnTypes: out, IsVariadic: t.IsVariadic()}, nil

	case reflect.Chan:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("failed to encode channel %v, bad element type: %v", t, err)
		}
		dir := encodeChanDir(t.ChanDir())
		return &v1.Type{Kind: v1.Type_CHAN, Element: elm, ChanDir: dir}, nil

	case reflect.Ptr:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("failed to encode pointer %v, bad base type: %v", t, err)
		}
		return &v1.Type{Kind: v1.Type_PTR, Element: elm}, nil

	default:
		return nil, fmt.Errorf("unencodable type %v", t)
	}
}

func tryEncodeSpecial(t reflect.Type) (v1.Type_Special, bool) {
	switch t {
	case reflectx.Error:
		return v1.Type_ERROR, true
	case reflectx.Context:
		return v1.Type_CONTEXT, true
	case reflectx.Type:
		return v1.Type_TYPE, true

	case typex.EventTimeType:
		return v1.Type_EVENTTIME, true
	case typex.WindowType:
		return v1.Type_WINDOW, true
	case typex.KVType:
		return v1.Type_KV, true
	case typex.CoGBKType:
		return v1.Type_COGBK, true
	case typex.WindowedValueType:
		return v1.Type_WINDOWEDVALUE, true

	case typex.TType:
		return v1.Type_T, true
	case typex.UType:
		return v1.Type_U, true
	case typex.VType:
		return v1.Type_V, true
	case typex.WType:
		return v1.Type_W, true
	case typex.XType:
		return v1.Type_X, true
	case typex.YType:
		return v1.Type_Y, true
	case typex.ZType:
		return v1.Type_Z, true

	default:
		return v1.Type_ILLEGAL, false
	}
}

func decodeType(t *v1.Type) (reflect.Type, error) {
	if t == nil {
		return nil, fmt.Errorf("failed to decode type %v, empty type", t)
	}

	switch t.Kind {
	case v1.Type_BOOL:
		return reflectx.Bool, nil
	case v1.Type_INT:
		return reflectx.Int, nil
	case v1.Type_INT8:
		return reflectx.Int8, nil
	case v1.Type_INT16:
		return reflectx.Int16, nil
	case v1.Type_INT32:
		return reflectx.Int32, nil
	case v1.Type_INT64:
		return reflectx.Int64, nil
	case v1.Type_UINT:
		return reflectx.Uint, nil
	case v1.Type_UINT8:
		return reflectx.Uint8, nil
	case v1.Type_UINT16:
		return reflectx.Uint16, nil
	case v1.Type_UINT32:
		return reflectx.Uint32, nil
	case v1.Type_UINT64:
		return reflectx.Uint64, nil
	case v1.Type_FLOAT32:
		return reflectx.Float32, nil
	case v1.Type_FLOAT64:
		return reflectx.Float64, nil
	case v1.Type_STRING:
		return reflectx.String, nil

	case v1.Type_SLICE:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad element: %v", t, err)
		}
		return reflect.SliceOf(elm), nil

	case v1.Type_STRUCT:
		var fields []reflect.StructField
		for _, f := range t.Fields {
			fType, err := decodeType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("failed to decode type %v, bad field type: %v", t, err)
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

	case v1.Type_FUNC:
		in, err := decodeTypes(t.GetParameterTypes())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad parameter type: %v", t, err)
		}
		out, err := decodeTypes(t.GetReturnTypes())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad return type: %v", t, err)
		}
		return reflect.FuncOf(in, out, t.GetIsVariadic()), nil

	case v1.Type_CHAN:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad element: %v", t, err)
		}
		dir, err := decodeChanDir(t.GetChanDir())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad channel direction: %v", t, err)
		}
		return reflect.ChanOf(dir, elm), nil

	case v1.Type_PTR:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad element: %v", t, err)
		}
		return reflect.PtrTo(elm), nil

	case v1.Type_SPECIAL:
		ret, err := decodeSpecial(t.Special)
		if err != nil {
			return nil, fmt.Errorf("failed to decode type %v, bad element: %v", t, err)
		}
		return ret, nil

	case v1.Type_EXTERNAL:
		ret, ok := runtime.LookupType(t.ExternalKey)
		if !ok {
			return nil, fmt.Errorf("failed to decode type %v, external key not found %v", t, t.ExternalKey)
		}
		return ret, nil

	default:
		return nil, fmt.Errorf("failed to decode type %v, unexpected type kind %v", t, t.Kind)
	}
}

func decodeSpecial(s v1.Type_Special) (reflect.Type, error) {
	switch s {
	case v1.Type_ERROR:
		return reflectx.Error, nil
	case v1.Type_CONTEXT:
		return reflectx.Context, nil
	case v1.Type_TYPE:
		return reflectx.Type, nil

	case v1.Type_EVENTTIME:
		return typex.EventTimeType, nil
	case v1.Type_WINDOW:
		return typex.WindowType, nil
	case v1.Type_KV:
		return typex.KVType, nil
	case v1.Type_COGBK:
		return typex.CoGBKType, nil
	case v1.Type_WINDOWEDVALUE:
		return typex.WindowedValueType, nil

	case v1.Type_T:
		return typex.TType, nil
	case v1.Type_U:
		return typex.UType, nil
	case v1.Type_V:
		return typex.VType, nil
	case v1.Type_W:
		return typex.WType, nil
	case v1.Type_X:
		return typex.XType, nil
	case v1.Type_Y:
		return typex.YType, nil
	case v1.Type_Z:
		return typex.ZType, nil

	default:
		return nil, fmt.Errorf("failed to decode special type, unknown type %v", s)
	}
}

func decodeTypes(list []*v1.Type) ([]reflect.Type, error) {
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

func encodeChanDir(dir reflect.ChanDir) v1.Type_ChanDir {
	switch dir {
	case reflect.RecvDir:
		return v1.Type_RECV
	case reflect.SendDir:
		return v1.Type_SEND
	case reflect.BothDir:
		return v1.Type_BOTH
	default:
		panic(fmt.Sprintf("Failed to encode channel direction, invalid value: %v", dir))
	}
}

func decodeChanDir(dir v1.Type_ChanDir) (reflect.ChanDir, error) {
	switch dir {
	case v1.Type_RECV:
		return reflect.RecvDir, nil
	case v1.Type_SEND:
		return reflect.SendDir, nil
	case v1.Type_BOTH:
		return reflect.BothDir, nil
	default:
		return reflect.BothDir, fmt.Errorf("failed to decode channel direction, invalid value: %v", dir)
	}
}

func encodeInputKind(k graph.InputKind) v1.MultiEdge_Inbound_InputKind {
	switch k {
	case graph.Main:
		return v1.MultiEdge_Inbound_MAIN
	case graph.Singleton:
		return v1.MultiEdge_Inbound_SINGLETON
	case graph.Slice:
		return v1.MultiEdge_Inbound_SLICE
	case graph.Map:
		return v1.MultiEdge_Inbound_MAP
	case graph.MultiMap:
		return v1.MultiEdge_Inbound_MULTIMAP
	case graph.Iter:
		return v1.MultiEdge_Inbound_ITER
	case graph.ReIter:
		return v1.MultiEdge_Inbound_REITER
	default:
		panic(fmt.Sprintf("Failed to encode input kind, invalid value: %v", k))
	}
}

func decodeInputKind(k v1.MultiEdge_Inbound_InputKind) (graph.InputKind, error) {
	switch k {
	case v1.MultiEdge_Inbound_MAIN:
		return graph.Main, nil
	case v1.MultiEdge_Inbound_SINGLETON:
		return graph.Singleton, nil
	case v1.MultiEdge_Inbound_SLICE:
		return graph.Slice, nil
	case v1.MultiEdge_Inbound_MAP:
		return graph.Map, nil
	case v1.MultiEdge_Inbound_MULTIMAP:
		return graph.MultiMap, nil
	case v1.MultiEdge_Inbound_ITER:
		return graph.Iter, nil
	case v1.MultiEdge_Inbound_REITER:
		return graph.ReIter, nil
	default:
		return graph.Main, fmt.Errorf("failed to decode input kind, invalid value: %v", k)
	}
}
