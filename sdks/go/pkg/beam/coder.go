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

package beam

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/golang/protobuf/proto"
)

func init() {
	RegisterFunction(JSONDec)
	RegisterFunction(JSONEnc)
	RegisterFunction(ProtoEnc)
	RegisterFunction(ProtoDec)
}

// Coder defines how to encode and decode values of type 'A' into byte streams.
// Coders are attached to PCollections of the same type. For PCollections
// consumed by GBK, the attached coders are required to be deterministic.
type Coder struct {
	coder *coder.Coder
}

// IsValid returns true iff the Coder is valid. Any use of an invalid Coder
// will result in a panic.
func (c Coder) IsValid() bool {
	return c.coder != nil
}

// Type returns the full type 'A' of elements the coder can encode and decode.
// 'A' must be a concrete full type, such as int or KV<int,string>.
func (c Coder) Type() FullType {
	if !c.IsValid() {
		panic("Invalid Coder")
	}
	return c.coder.T
}

func (c Coder) String() string {
	if c.coder == nil {
		return "$"
	}
	return c.coder.String()
}

// TODO(herohde) 4/4/2017: for convenience, we use the magic json coding
// everywhere. To be replaced by Coder registry, sharing, etc.

// TODO: select optimal coder based on type, notably handling int, string, etc.

// TODO(herohde) 7/11/2017: figure out best way to let transformation use
// coders (like passert). For now, we just allow them to grab in the internal
// coder. Maybe it's cleaner to pull Encode/Decode into beam instead, if
// adequate. The issue is that we would need non-windowed coding. Maybe focus on
// coder registry and construction: then type -> coder might be adequate.

// UnwrapCoder returns the internal coder.
func UnwrapCoder(c Coder) *coder.Coder {
	return c.coder
}

// NewCoder infers a Coder for any bound full type.
func NewCoder(t FullType) Coder {
	c, err := inferCoder(t)
	if err != nil {
		panic(err) // for now
	}
	return Coder{c}
}

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

func inferCoder(t FullType) (*coder.Coder, error) {
	switch t.Class() {
	case typex.Concrete, typex.Container:
		switch t.Type() {
		case reflectx.Int, reflectx.Int8, reflectx.Int16, reflectx.Int32, reflectx.Int64:
			c, err := coderx.NewVarIntZ(t.Type())
			if err != nil {
				return nil, err
			}
			return &coder.Coder{Kind: coder.Custom, T: t, Custom: c}, nil
		case reflectx.Uint, reflectx.Uint8, reflectx.Uint16, reflectx.Uint32, reflectx.Uint64:
			c, err := coderx.NewVarUintZ(t.Type())
			if err != nil {
				return nil, err
			}
			return &coder.Coder{Kind: coder.Custom, T: t, Custom: c}, nil
		case reflectx.String, reflectx.ByteSlice:
			// TODO(BEAM-3580): we should stop encoding string using the bytecoder. It forces
			// conversions at runtime in inconvenient places.
			return &coder.Coder{Kind: coder.Bytes, T: t}, nil
		default:
			// TODO(BEAM-3306): the coder registry should be consulted here for user
			// specified types and their coders.
			if t.Type().Implements(protoMessageType) {
				c, err := newProtoCoder(t.Type())
				if err != nil {
					return nil, err
				}
				return &coder.Coder{Kind: coder.Custom, T: t, Custom: c}, nil
			}

			c, err := newJSONCoder(t.Type())
			if err != nil {
				return nil, err
			}
			return &coder.Coder{Kind: coder.Custom, T: t, Custom: c}, nil
		}

	case typex.Composite:
		c, err := inferCoders(t.Components())
		if err != nil {
			return nil, err
		}

		switch t.Type() {
		case typex.KVType:
			return &coder.Coder{Kind: coder.KV, T: t, Components: c}, nil
		case typex.CoGBKType:
			return &coder.Coder{Kind: coder.CoGBK, T: t, Components: c}, nil
		case typex.WindowedValueType:
			return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: c, Window: window.NewGlobalWindow()}, nil

		default:
			panic(fmt.Sprintf("Unexpected composite type: %v", t))
		}
	default:
		panic(fmt.Sprintf("Unexpected type: %v", t))
	}
}

func inferCoders(list []FullType) ([]*coder.Coder, error) {
	var ret []*coder.Coder
	for _, t := range list {
		c, err := inferCoder(t)
		if err != nil {
			return nil, err
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// TODO(herohde) 4/5/2017: decide whether we want an Encoded form. For now,
// we'll use exploded form coders only using typex.T. We might also need a
// form that doesn't require LengthPrefix'ing to cut up the bytestream from
// the FnHarness.

// ProtoEnc marshals the supplied proto.Message.
func ProtoEnc(in typex.T) ([]byte, error) {
	return proto.Marshal(in.(proto.Message))
}

// ProtoDec unmarshals the supplied bytes into an instance of the supplied
// proto.Message type.
func ProtoDec(t reflect.Type, in []byte) (typex.T, error) {
	val := reflect.New(t.Elem()).Interface().(proto.Message)
	if err := proto.Unmarshal(in, val); err != nil {
		return nil, err
	}
	return val, nil
}

func newProtoCoder(t reflect.Type) (*coder.CustomCoder, error) {
	c, err := coder.NewCustomCoder("proto", t, ProtoEnc, ProtoDec)
	if err != nil {
		return nil, fmt.Errorf("invalid coder: %v", err)
	}
	return c, nil
}

// Concrete and universal custom coders both have a similar signature.
// Conversion is handled by reflection.

// JSONEnc encodes the supplied value in JSON.
func JSONEnc(in typex.T) ([]byte, error) {
	return json.Marshal(in)
}

// JSONDec decodes the supplied JSON into an instance of the supplied type.
func JSONDec(t reflect.Type, in []byte) (T, error) {
	val := reflect.New(t)
	if err := json.Unmarshal(in, val.Interface()); err != nil {
		return nil, err
	}
	return val.Elem().Interface(), nil
}

func newJSONCoder(t reflect.Type) (*coder.CustomCoder, error) {
	c, err := coder.NewCustomCoder("json", t, JSONEnc, JSONDec)
	if err != nil {
		return nil, fmt.Errorf("invalid coder: %v", err)
	}
	return c, nil
}
