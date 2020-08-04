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
	"reflect"

	"encoding/json"

	"encoding/base64"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	v1pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
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
	var ref v1pb.Type
	if err := protox.DecodeBase64(data, &ref); err != nil {
		return nil, err
	}
	return decodeType(&ref)
}

// EncodeFn encodes a function and parameter types as a string. The function
// symbol must be resolvable via the runtime.GlobalSymbolResolver. The types must
// be encodable.
func EncodeFn(fn reflectx.Func) (string, error) {
	u, err := funcx.New(fn)
	if err != nil {
		return "", err
	}
	ref, err := encodeUserFn(u)
	if err != nil {
		return "", err
	}
	return protox.EncodeBase64(ref)
}

// DecodeFn encodes a function. The function symbol must be resolvable via the
// runtime.GlobalSymbolResolver. The parameter types must be encodable.
func DecodeFn(data string) (reflectx.Func, error) {
	var ref v1pb.UserFn
	if err := protox.DecodeBase64(data, &ref); err != nil {
		return nil, err
	}
	fn, err := decodeUserFn(&ref)
	if err != nil {
		return nil, err
	}
	return reflectx.MakeFunc(fn), nil
}

// EncodeCoder encodes a coder as a string. Any custom coder function
// symbol must be resolvable via the runtime.GlobalSymbolResolver. The types must
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
// resolvable via the runtime.GlobalSymbolResolver. The types must be encodable.
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
