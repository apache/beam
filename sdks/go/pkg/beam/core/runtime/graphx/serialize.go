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

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
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
			return nil, fmt.Errorf("encode: bad userfn: %v", err)
		}
		ret.Fn = ref
	}

	if edge.CombineFn != nil {
		ref, err := encodeFn((*graph.Fn)(edge.CombineFn))
		if err != nil {
			return nil, fmt.Errorf("encode: bad combinefn: %v", err)
		}
		ret.Fn = ref
	}

	for _, in := range edge.Input {
		kind := encodeInputKind(in.Kind)
		t, err := encodeFullType(in.Type)
		if err != nil {
			return nil, fmt.Errorf("encode: bad input type: %v", err)
		}
		ret.Inbound = append(ret.Inbound, &v1.MultiEdge_Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Output {
		t, err := encodeFullType(out.Type)
		if err != nil {
			return nil, fmt.Errorf("encode: bad output type: %v", err)
		}
		ret.Outbound = append(ret.Outbound, &v1.MultiEdge_Outbound{Type: t})
	}
	return ret, nil
}

// DecodeMultiEdge converts the wire representation into the preprocessed
// components representing that edge. We deserialize to components to avoid
// inserting the edge into a graph or creating a detached edge.
func DecodeMultiEdge(edge *v1.MultiEdge) (graph.Opcode, *graph.Fn, []*graph.Inbound, []*graph.Outbound, error) {
	var u *graph.Fn
	var inbound []*graph.Inbound
	var outbound []*graph.Outbound

	opcode := graph.Opcode(edge.Opcode)

	if edge.Fn != nil {
		var err error
		u, err = decodeFn(edge.Fn)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("decode: bad userfn: %v", err)
		}
	}
	for _, in := range edge.Inbound {
		kind, err := decodeInputKind(in.Kind)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("decode: bad input kind: %v", err)
		}
		t, err := decodeFullType(in.Type)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("decode: bad input type: %v", err)
		}
		inbound = append(inbound, &graph.Inbound{Kind: kind, Type: t})
	}
	for _, out := range edge.Outbound {
		t, err := decodeFullType(out.Type)
		if err != nil {
			return "", nil, nil, nil, fmt.Errorf("decode: bad output type: %v", err)
		}
		outbound = append(outbound, &graph.Outbound{Type: t})
	}

	return opcode, u, inbound, outbound, nil
}

func encodeCustomCoder(c *coder.CustomCoder) (*v1.CustomCoder, error) {
	t, err := encodeType(c.Type)
	if err != nil {
		return nil, fmt.Errorf("bad underlying type: %v", err)
	}
	enc, err := EncodeUserFn(c.Enc)
	if err != nil {
		return nil, fmt.Errorf("bad enc: %v", err)
	}
	dec, err := EncodeUserFn(c.Dec)
	if err != nil {
		return nil, fmt.Errorf("bad dec: %v", err)
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
		return nil, fmt.Errorf("bad type: %v", err)
	}
	enc, err := DecodeUserFn(c.Enc)
	if err != nil {
		return nil, fmt.Errorf("bad dec: %v", err)
	}
	dec, err := DecodeUserFn(c.Dec)
	if err != nil {
		return nil, fmt.Errorf("bad dec: %v", err)
	}

	ret := &coder.CustomCoder{
		Name: c.Name,
		Type: t,
		Enc:  enc,
		Dec:  dec,
	}
	return ret, nil
}

func encodeFn(u *graph.Fn) (*v1.Fn, error) {
	switch {
	case u.DynFn != nil:
		gen := reflectx.FunctionName(u.DynFn.Gen)
		t, err := encodeType(u.DynFn.T)
		if err != nil {
			return nil, fmt.Errorf("bad function type: %v", err)
		}
		return &v1.Fn{Dynfn: &v1.DynFn{
			Name: u.DynFn.Name,
			Type: t,
			Data: u.DynFn.Data,
			Gen:  gen,
		}}, nil

	case u.Fn != nil:
		fn, err := EncodeUserFn(u.Fn)
		if err != nil {
			return nil, fmt.Errorf("bad userfn: %v", err)
		}
		return &v1.Fn{Fn: fn}, nil

	case u.Recv != nil:
		t := reflect.TypeOf(u.Recv)
		k, ok := runtime.TypeKey(reflectx.SkipPtr(t))
		if !ok {
			return nil, fmt.Errorf("bad recv: %v", u.Recv)
		}
		if _, ok := runtime.LookupType(k); !ok {
			return nil, fmt.Errorf("recv type must be registered: %v", t)
		}
		typ, err := encodeType(t)
		if err != nil {
			panic(fmt.Sprintf("bad recv type: %v", u.Recv))
		}

		data, err := json.Marshal(u.Recv)
		if err != nil {
			return nil, fmt.Errorf("bad userfn: %v", err)
		}
		return &v1.Fn{Type: typ, Opt: string(data)}, nil

	default:
		panic("empty Fn")
	}
}

func decodeFn(u *v1.Fn) (*graph.Fn, error) {
	if u.Dynfn != nil {
		gen, err := runtime.ResolveFunction(u.Dynfn.Gen, genFnType)
		if err != nil {
			return nil, fmt.Errorf("bad symbol %v: %v", u.Dynfn.Gen, err)
		}

		t, err := decodeType(u.Dynfn.Type)
		if err != nil {
			return nil, fmt.Errorf("bad type: %v", err)
		}
		return graph.NewFn(&graph.DynFn{
			Name: u.Dynfn.Name,
			T:    t,
			Data: u.Dynfn.Data,
			Gen:  gen.(func(string, reflect.Type, []byte) reflectx.Func),
		})
	}
	if u.Fn != nil {
		fn, err := DecodeUserFn(u.Fn)
		if err != nil {
			return nil, fmt.Errorf("bad userfn: %v", err)
		}
		return &graph.Fn{Fn: fn}, nil
	}

	t, err := decodeType(u.Type)
	if err != nil {
		return nil, fmt.Errorf("bad type: %v", err)
	}
	fn, err := reflectx.UnmarshalJSON(t, u.Opt)
	if err != nil {
		return nil, fmt.Errorf("bad struct encoding: %v", err)
	}
	return graph.NewFn(fn)
}

// EncodeUserFn translates the preprocessed representation of a Beam user function
// into the wire representation, capturing all the inputs and outputs needed.
func EncodeUserFn(u *funcx.Fn) (*v1.UserFn, error) {
	// TODO(herohde) 5/23/2017: reject closures and dynamic functions. They can't
	// be serialized.

	symbol := u.Fn.Name()
	t, err := encodeType(u.Fn.Type())
	if err != nil {
		return nil, fmt.Errorf("encode: bad function type: %v", err)
	}
	return &v1.UserFn{Name: symbol, Type: t}, nil
}

// DecodeUserFn receives the wire representation of a Beam user function,
// extracting the preprocessed representation, expanding all inputs and outputs
// of the function.
func DecodeUserFn(ref *v1.UserFn) (*funcx.Fn, error) {
	t, err := decodeType(ref.GetType())
	if err != nil {
		return nil, err
	}

	fn, err := runtime.ResolveFunction(ref.Name, t)
	if err != nil {
		return nil, fmt.Errorf("decode: failed to find symbol %v: %v", ref.Name, err)
	}
	return funcx.New(reflectx.MakeFunc(fn))
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
		return nil, fmt.Errorf("bad type: %v", err)
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
		return nil, fmt.Errorf("bad type: %v", err)
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
	case reflect.String:
		return &v1.Type{Kind: v1.Type_STRING}, nil

	case reflect.Slice:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		return &v1.Type{Kind: v1.Type_SLICE, Element: elm}, nil

	case reflect.Struct:
		var fields []*v1.Type_StructField
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)

			fType, err := encodeType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("bad field type: %v", err)
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
				return nil, fmt.Errorf("bad parameter type: %v", err)
			}
			in = append(in, param)
		}
		var out []*v1.Type
		for i := 0; i < t.NumOut(); i++ {
			ret, err := encodeType(t.Out(i))
			if err != nil {
				return nil, fmt.Errorf("bad return type: %v", err)
			}
			out = append(out, ret)
		}
		return &v1.Type{Kind: v1.Type_FUNC, ParameterTypes: in, ReturnTypes: out, IsVariadic: t.IsVariadic()}, nil

	case reflect.Chan:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		dir := encodeChanDir(t.ChanDir())
		return &v1.Type{Kind: v1.Type_CHAN, Element: elm, ChanDir: dir}, nil

	case reflect.Ptr:
		elm, err := encodeType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		return &v1.Type{Kind: v1.Type_PTR, Element: elm}, nil

	default:
		return nil, fmt.Errorf("unencodable type: %v", t)
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
		return nil, fmt.Errorf("empty type")
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
	case v1.Type_STRING:
		return reflectx.String, nil

	case v1.Type_SLICE:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		return reflect.SliceOf(elm), nil

	case v1.Type_STRUCT:
		var fields []reflect.StructField
		for _, f := range t.Fields {
			fType, err := decodeType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("bad field type: %v", err)
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
			return nil, fmt.Errorf("bad parameter type: %v", err)
		}
		out, err := decodeTypes(t.GetReturnTypes())
		if err != nil {
			return nil, fmt.Errorf("bad return type: %v", err)
		}
		return reflect.FuncOf(in, out, t.GetIsVariadic()), nil

	case v1.Type_CHAN:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		dir, err := decodeChanDir(t.GetChanDir())
		if err != nil {
			return nil, fmt.Errorf("bad ChanDir: %v", err)
		}
		return reflect.ChanOf(dir, elm), nil

	case v1.Type_PTR:
		elm, err := decodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		return reflect.PtrTo(elm), nil

	case v1.Type_SPECIAL:
		ret, err := decodeSpecial(t.Special)
		if err != nil {
			return nil, fmt.Errorf("bad element: %v", err)
		}
		return ret, nil

	case v1.Type_EXTERNAL:
		ret, ok := runtime.LookupType(t.ExternalKey)
		if !ok {
			return nil, fmt.Errorf("external key not found: %v", t.ExternalKey)
		}
		return ret, nil

	default:
		return nil, fmt.Errorf("unexpected type kind: %v", t.Kind)
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
		return nil, fmt.Errorf("unknown special type: %v", s)
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
		panic(fmt.Sprintf("unexpected reflect.ChanDir: %v", dir))
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
		return reflect.BothDir, fmt.Errorf("invalid chan dir: %v", dir)
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
		panic(fmt.Sprintf("unexpected input: %v", k))
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
		return graph.Main, fmt.Errorf("invalid input kind: %v", k)
	}
}

// CoderRef defines the (structured) Coder in serializable form. It is
// an artifact of the CloudObject encoding.
type CoderRef struct {
	Type         string      `json:"@type,omitempty"`
	Components   []*CoderRef `json:"component_encodings,omitempty"`
	IsWrapper    bool        `json:"is_wrapper,omitempty"`
	IsPairLike   bool        `json:"is_pair_like,omitempty"`
	IsStreamLike bool        `json:"is_stream_like,omitempty"`
}

// Exported types are used for translation lookup.
const (
	WindowedValueType = "kind:windowed_value"
	BytesType         = "kind:bytes"
	VarIntType        = "kind:varint"
	GlobalWindowType  = "kind:global_window"
	streamType        = "kind:stream"
	pairType          = "kind:pair"
	lengthPrefixType  = "kind:length_prefix"

	cogbklistType = "kind:cogbklist" // CoGBK representation. Not a coder.
)

// WrapExtraWindowedValue adds an additional WV needed for side input, which
// expects the coder to have exactly one component with the element.
func WrapExtraWindowedValue(c *CoderRef) *CoderRef {
	return &CoderRef{Type: WindowedValueType, Components: []*CoderRef{c}}
}

// EncodeCoderRefs returns the encoded forms understood by the runner.
func EncodeCoderRefs(list []*coder.Coder) ([]*CoderRef, error) {
	var refs []*CoderRef
	for _, c := range list {
		ref, err := EncodeCoderRef(c)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// EncodeCoderRef returns the encoded form understood by the runner.
func EncodeCoderRef(c *coder.Coder) (*CoderRef, error) {
	switch c.Kind {
	case coder.Custom:
		ref, err := encodeCustomCoder(c.Custom)
		if err != nil {
			return nil, err
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: lengthPrefixType, Components: []*CoderRef{{Type: data}}}, nil

	case coder.KV:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("bad KV: %v", c)
		}

		key, err := EncodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		value, err := EncodeCoderRef(c.Components[1])
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: pairType, Components: []*CoderRef{key, value}, IsPairLike: true}, nil

	case coder.CoGBK:
		if len(c.Components) < 2 {
			return nil, fmt.Errorf("bad CoGBK: %v", c)
		}

		refs, err := EncodeCoderRefs(c.Components)
		if err != nil {
			return nil, err
		}

		value := refs[1]
		if len(c.Components) > 2 {
			// TODO(BEAM-490): don't inject union coder for CoGBK.

			union := &CoderRef{Type: cogbklistType, Components: refs[1:]}
			value = &CoderRef{Type: lengthPrefixType, Components: []*CoderRef{union}}
		}

		stream := &CoderRef{Type: streamType, Components: []*CoderRef{value}, IsStreamLike: true}
		return &CoderRef{Type: pairType, Components: []*CoderRef{refs[0], stream}, IsPairLike: true}, nil

	case coder.WindowedValue:
		if len(c.Components) != 1 || c.Window == nil {
			return nil, fmt.Errorf("bad windowed value: %v", c)
		}

		elm, err := EncodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		w, err := encodeWindow(c.Window)
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: WindowedValueType, Components: []*CoderRef{elm, w}, IsWrapper: true}, nil

	case coder.Bytes:
		// TODO(herohde) 6/27/2017: add length-prefix and not assume nested by context?
		return &CoderRef{Type: BytesType}, nil

	case coder.VarInt:
		return &CoderRef{Type: VarIntType}, nil

	default:
		return nil, fmt.Errorf("bad coder kind: %v", c.Kind)
	}
}

// DecodeCoderRefs extracts usable coders from the encoded runner form.
func DecodeCoderRefs(list []*CoderRef) ([]*coder.Coder, error) {
	var ret []*coder.Coder
	for _, ref := range list {
		c, err := DecodeCoderRef(ref)
		if err != nil {
			return nil, err
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// DecodeCoderRef extracts a usable coder from the encoded runner form.
func DecodeCoderRef(c *CoderRef) (*coder.Coder, error) {
	switch c.Type {
	case BytesType:
		return coder.NewBytes(), nil

	case VarIntType:
		return coder.NewVarInt(), nil

	case pairType:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("bad pair: %+v", c)
		}

		key, err := DecodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}

		elm := c.Components[1]
		kind := coder.KV
		root := typex.KVType

		isGBK := elm.Type == streamType
		if isGBK {
			elm = elm.Components[0]
			kind = coder.CoGBK
			root = typex.CoGBKType

			// TODO(BEAM-490): If CoGBK with > 1 input, handle as special GBK. We expect
			// it to be encoded as CoGBK<K,LP<Union<V,W,..>>. Remove this handling once
			// CoGBK has a first-class representation.

			if refs, ok := isCoGBKList(elm); ok {
				values, err := DecodeCoderRefs(refs)
				if err != nil {
					return nil, err
				}

				t := typex.New(root, append([]typex.FullType{key.T}, coder.Types(values)...)...)
				return &coder.Coder{Kind: kind, T: t, Components: append([]*coder.Coder{key}, values...)}, nil
			}
		}

		value, err := DecodeCoderRef(elm)
		if err != nil {
			return nil, err
		}

		t := typex.New(root, key.T, value.T)
		return &coder.Coder{Kind: kind, T: t, Components: []*coder.Coder{key, value}}, nil

	case lengthPrefixType:
		if len(c.Components) != 1 {
			return nil, fmt.Errorf("bad length prefix: %+v", c)
		}

		var ref v1.CustomCoder
		if err := protox.DecodeBase64(c.Components[0].Type, &ref); err != nil {
			return nil, fmt.Errorf("base64 decode for %v failed: %v", c.Components[0].Type, err)
		}
		custom, err := decodeCustomCoder(&ref)
		if err != nil {
			return nil, err
		}
		t := typex.New(custom.Type)
		return &coder.Coder{Kind: coder.Custom, T: t, Custom: custom}, nil

	case WindowedValueType:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("bad windowed value: %+v", c)
		}

		elm, err := DecodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		w, err := decodeWindow(c.Components[1])
		if err != nil {
			return nil, err
		}
		t := typex.New(typex.WindowedValueType, elm.T)

		return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: []*coder.Coder{elm}, Window: w}, nil

	case streamType:
		return nil, fmt.Errorf("stream must be pair value: %+v", c)

	default:
		return nil, fmt.Errorf("custom coders must be length prefixed: %+v", c)
	}
}

func isCoGBKList(ref *CoderRef) ([]*CoderRef, bool) {
	if ref.Type != lengthPrefixType {
		return nil, false
	}
	ref2 := ref.Components[0]
	if ref2.Type != cogbklistType {
		return nil, false
	}
	return ref2.Components, true
}

// TODO(wcn): Windowing information isn't currently propagated through
// our code. These methods will be used by other packages, so exporting
// them now.

// encodeWindow translates the preprocessed representation of a Beam coder
// into the wire representation, capturing the underlying types used by
// the coder.
func encodeWindow(w *window.Window) (*CoderRef, error) {
	switch w.Kind() {
	case window.GlobalWindow:
		return &CoderRef{Type: GlobalWindowType}, nil
	default:
		return nil, fmt.Errorf("bad window kind: %v", w.Kind())
	}
}

// decodeWindow receives the wire representation of a Beam coder, extracting
// the preprocessed representation, expanding all types used by the coder.
func decodeWindow(w *CoderRef) (*window.Window, error) {
	switch w.Type {
	case GlobalWindowType:
		return window.NewGlobalWindow(), nil
	default:
		return nil, fmt.Errorf("bad window: %v", w.Type)
	}
}
