package graph

import (
	"debug/elf"
	"encoding/json"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"reflect"
	"runtime"
	"unsafe"
)

func EncodeMultiEdge(edge *MultiEdge) (*v1.MultiEdge, error) {
	ret := &v1.MultiEdge{}

	if edge.DoFn != nil {
		ref, err := EncodeFnRef(edge.DoFn.Fn.Interface())
		if err != nil {
			return nil, fmt.Errorf("Bad userfn: %v", err)
		}
		ret.UserFn = ref
	}
	if edge.Data != nil {
		data, err := json.Marshal(edge.Data)
		if err != nil {
			return nil, fmt.Errorf("Bad data: %v", err)
		}
		ret.Data = string(data)
	}
	for _, in := range edge.Input {
		t, err := EncodeType(in.T)
		if err != nil {
			return nil, fmt.Errorf("Bad input type: %v", err)
		}
		ret.Inbound = append(ret.Inbound, &v1.MultiEdge_Inbound{Type: t})
	}
	for _, out := range edge.Output {
		t, err := EncodeType(out.T)
		if err != nil {
			return nil, fmt.Errorf("Bad output type: %v", err)
		}
		real, err := EncodeType(out.To.T)
		if err != nil {
			return nil, fmt.Errorf("Bad node type: %v", err)
		}
		ret.Outbound = append(ret.Outbound, &v1.MultiEdge_Outbound{Type: t, Node: &v1.Node{real}})
	}
	return ret, nil
}

func EncodeFnRef(fn interface{}) (*v1.FunctionRef, error) {
	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		return nil, fmt.Errorf("Input not a function: %v", val.Kind())
	}

	symbol := runtime.FuncForPC(uintptr(val.Pointer())).Name()
	t, err := EncodeType(val.Type())
	if err != nil {
		return nil, fmt.Errorf("Bad function type: %v", err)
	}
	return &v1.FunctionRef{Name: symbol, Type: t}, nil
}

func DecodeFnRef(ref *v1.FunctionRef) (interface{}, error) {
	t, err := DecodeType(ref.GetType())
	if err != nil {
		return nil, err
	}

	// NOTE: we assume decoding on Linux, for now.

	ptr, err := sym2addr(ref.Name)
	if err != nil {
		return nil, fmt.Errorf("Failed to find symbol %v: %v", ref.Name, err)
	}
	v := reflect.New(t).Elem()
	*(*uintptr)(unsafe.Pointer(v.Addr().Pointer())) = (uintptr)(unsafe.Pointer(&ptr))

	return v.Interface(), nil
}

func sym2addr(name string) (uintptr, error) {
	f, err := elf.Open("/proc/self/exe")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	syms, err := f.Symbols()
	if err != nil {
		return 0, err
	}

	for _, s := range syms {
		if s.Name == name {
			return uintptr(s.Value), nil
		}
	}
	return 0, fmt.Errorf("no symbol %q", name)
}

func EncodeType(t reflect.Type) (*v1.Type, error) {
	return encodeType(t, false)
}

func encodeType(t reflect.Type, dataField bool) (*v1.Type, error) {
	if t == reflectx.Error {
		return &v1.Type{Kind: v1.Type_ERROR}, nil
	}
	if t == reflectx.ReflectValue {
		return &v1.Type{Kind: v1.Type_UNIVERSAL}, nil
	}

	// Special handling of datafield values.
	if t == DataFnValueType && dataField {
		return &v1.Type{Kind: v1.Type_FNVALUE}, nil
	}

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
		elm, err := encodeType(t.Elem(), dataField)
		if err != nil {
			return nil, fmt.Errorf("Bad element: %v", err)
		}
		return &v1.Type{Kind: v1.Type_SLICE, Element: elm}, nil
	case reflect.Struct:
		var fields []*v1.Type_StructField
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			fType, err := encodeType(f.Type, dataField || reflectx.HasTag(f, reflectx.DataTag))
			if err != nil {
				return nil, fmt.Errorf("Bad field type: %v", err)
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
			param, err := encodeType(t.In(i), dataField)
			if err != nil {
				return nil, fmt.Errorf("Bad parameter type: %v", err)
			}
			in = append(in, param)
		}
		var out []*v1.Type
		for i := 0; i < t.NumOut(); i++ {
			ret, err := encodeType(t.Out(i), dataField)
			if err != nil {
				return nil, fmt.Errorf("Bad return type: %v", err)
			}
			out = append(out, ret)
		}
		return &v1.Type{Kind: v1.Type_FUNC, ParameterTypes: in, ReturnTypes: out, IsVariadic: t.IsVariadic()}, nil
	case reflect.Chan:
		elm, err := encodeType(t.Elem(), dataField)
		if err != nil {
			return nil, fmt.Errorf("Bad element: %v", err)
		}
		dir := encodeChanDir(t.ChanDir())
		return &v1.Type{Kind: v1.Type_CHAN, Element: elm, ChanDir: dir}, nil

	default:
		return nil, fmt.Errorf("Unencodable type: %v", t)
	}
}

func DecodeType(t *v1.Type) (reflect.Type, error) {
	if t == nil {
		return nil, fmt.Errorf("Empty type")
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
		elm, err := DecodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("Bad element: %v", err)
		}
		return reflect.SliceOf(elm), nil

	case v1.Type_STRUCT:
		var fields []reflect.StructField
		for _, f := range t.Fields {
			fType, err := DecodeType(f.Type)
			if err != nil {
				return nil, fmt.Errorf("Bad field type: %v", err)
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
		in, err := DecodeTypes(t.GetParameterTypes())
		if err != nil {
			return nil, fmt.Errorf("Bad parameter type: %v", err)
		}
		out, err := DecodeTypes(t.GetReturnTypes())
		if err != nil {
			return nil, fmt.Errorf("Bad return type: %v", err)
		}
		return reflect.FuncOf(in, out, t.GetIsVariadic()), nil

	case v1.Type_CHAN:
		elm, err := DecodeType(t.GetElement())
		if err != nil {
			return nil, fmt.Errorf("Bad element: %v", err)
		}
		dir, err := decodeChanDir(t.GetChanDir())
		if err != nil {
			return nil, fmt.Errorf("Bad ChanDir: %v", err)
		}
		return reflect.ChanOf(dir, elm), nil

	case v1.Type_ERROR:
		return reflectx.Error, nil
	case v1.Type_UNIVERSAL:
		return reflectx.ReflectValue, nil

	case v1.Type_FNVALUE:
		return DataFnValueType, nil

	default:
		return nil, fmt.Errorf("Unexpected type kind: %v", t.Kind)
	}
}

func DecodeTypes(list []*v1.Type) ([]reflect.Type, error) {
	var ret []reflect.Type
	for _, elm := range list {
		t, err := DecodeType(elm)
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
		panic(fmt.Sprintf("Unexpected ChanDir: %v", dir))
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
		return reflect.BothDir, fmt.Errorf("Invalid chan dir: %v", dir)

	}
}
