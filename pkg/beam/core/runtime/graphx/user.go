package graphx

import (
	"reflect"

	"encoding/json"

	"encoding/base64"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
)

// EncodeType encodes a type as a string. Unless registered, the decoded type
// is only guaranteed to be isomorphic to the input with regard to data members.
// The returned type will have no methods.
func EncodeType(t reflect.Type) (string, error) {
	ref, err := encodeType(t)
	if err != nil {
		return "", err
	}
	return protox.EncodeBase64(ref)
}

// DecodeType decodes a type. Unless registered, the decoded type is only
// guaranteed to be isomorphic to the input with regard to data members.
// The returned type will have no methods.
func DecodeType(data string) (reflect.Type, error) {
	var ref v1.Type
	if err := protox.DecodeBase64(data, &ref); err != nil {
		return nil, err
	}
	return decodeType(&ref)
}

// EncodeFn encodes a function and parameter types as a string. The function
// symbol must be resolvable via the runtime.SymbolResolver. The types must
// be encodable.
func EncodeFn(fn reflect.Value) (string, error) {
	u, err := funcx.New(fn.Interface())
	if err != nil {
		return "", err
	}
	ref, err := EncodeUserFn(u)
	if err != nil {
		return "", err
	}
	return protox.EncodeBase64(ref)
}

// DecodeFn encodes a function. The function symbol must be resolvable via the
// runtime.SymbolResolver. The parameter types must be encodable.
func DecodeFn(data string) (reflect.Value, error) {
	var ref v1.UserFn
	if err := protox.DecodeBase64(data, &ref); err != nil {
		return reflect.Value{}, err
	}
	fn, err := DecodeUserFn(&ref)
	if err != nil {
		return reflect.Value{}, err
	}
	return fn.Fn, nil
}

// EncodeCoder encodes a coder as a string. Any custom coder function
// symbol must be resolvable via the runtime.SymbolResolver. The types must
// be encodable.
func EncodeCoder(c *coder.Coder) (string, error) {
	ref, err := EncodeCoderRef(c)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(ref)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// DecodeCoder decodes a coder. Any custom coder function symbol must be
// resolvable via the runtime.SymbolResolver. The types must be encodable.
func DecodeCoder(data string) (*coder.Coder, error) {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	var ref CoderRef
	if err := json.Unmarshal(decoded, &ref); err != nil {
		return nil, err
	}
	return DecodeCoderRef(&ref)
}
