package graphx

import (
	"encoding/json"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/protox"
	"reflect"
)

// TODO(herohde) 4/24/2017: do this capabiltiy even add enough value?

var (
	DataFnValueType = reflect.TypeOf((*DataFnValue)(nil)).Elem()
)

// FnValue is a serialization-wrapper of a function reference. Given that the
// receiving end is only isomorphic, but has no methods, we cannot handle
// custom serialization in the data segment. We can, however, special-case a
// few convenient types.
type DataFnValue struct {
	Fn interface{}
}

func (f DataFnValue) MarshalJSON() ([]byte, error) {
	u, err := userfn.New(f.Fn)
	if err != nil {
		return nil, err
	}
	ref, err := EncodeUserFn(u)
	if err != nil {
		return nil, err
	}
	str, err := protox.EncodeBase64(ref)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

func (f *DataFnValue) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	var ref v1.UserFn
	if err := protox.DecodeBase64(s, &ref); err != nil {
		return err
	}
	fn, err := DecodeUserFn(&ref)
	if err != nil {
		return err
	}

	f.Fn = fn.Fn.Interface()
	return nil
}
