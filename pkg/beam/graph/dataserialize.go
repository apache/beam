package graph

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/v1"
	"reflect"
)

var DataFnValueType = reflect.TypeOf((*DataFnValue)(nil)).Elem()

// FnValue is a serialization-wrapper of a function reference. Given that the
// receiving end is only isomorphic, but has no methods, we cannot handle
// custom serialization in the data segment. We can, however, special-case a
// few convenient types.
type DataFnValue struct {
	Fn interface{}
}

func (f DataFnValue) MarshalJSON() ([]byte, error) {
	ref, err := EncodeFnRef(f.Fn)
	if err != nil {
		return nil, err
	}
	data, err := proto.Marshal(ref)
	if err != nil {
		return nil, err
	}
	str := base64.StdEncoding.EncodeToString(data)
	return json.Marshal(str)
}

func (f *DataFnValue) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("base64 decoding failed: %v", err)
	}
	var ref v1.FunctionRef
	if err := proto.Unmarshal(decoded, &ref); err != nil {
		return err
	}
	fn, err := DecodeFnRef(&ref)
	if err != nil {
		return err
	}

	f.Fn = fn
	return nil
}
