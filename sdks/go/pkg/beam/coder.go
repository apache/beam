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
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/coderx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type jsonCoder interface {
	json.Marshaler
	json.Unmarshaler
}

var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()
var protoReflectMessageType = reflect.TypeOf((*protoreflect.ProtoMessage)(nil)).Elem()
var jsonCoderType = reflect.TypeOf((*jsonCoder)(nil)).Elem()

func init() {
	coder.RegisterCoder(protoMessageType, protoEnc, protoDec)
	coder.RegisterCoder(protoReflectMessageType, protoEnc, protoDec)
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

// NewElementEncoder returns a new encoding function for the given type.
func NewElementEncoder(t reflect.Type) ElementEncoder {
	c, err := inferCoder(typex.New(t))
	if err != nil {
		panic(err)
	}
	return &execEncoder{enc: exec.MakeElementEncoder(c)}
}

// execEncoder wraps an exec.ElementEncoder to implement the ElementDecoder interface
// in this package.
type execEncoder struct {
	enc   exec.ElementEncoder
	coder *coder.Coder
}

func (e *execEncoder) Encode(element interface{}, w io.Writer) error {
	return e.enc.Encode(&exec.FullValue{Elm: element}, w)
}

func (e *execEncoder) String() string {
	return e.coder.String()
}

// NewElementDecoder returns an ElementDecoder the given type.
func NewElementDecoder(t reflect.Type) ElementDecoder {
	c, err := inferCoder(typex.New(t))
	if err != nil {
		panic(err)
	}
	return &execDecoder{dec: exec.MakeElementDecoder(c)}
}

// execDecoder wraps an exec.ElementDecoder to implement the ElementDecoder interface
// in this package.
type execDecoder struct {
	dec   exec.ElementDecoder
	coder *coder.Coder
}

func (d *execDecoder) Decode(r io.Reader) (interface{}, error) {
	fv, err := d.dec.Decode(r)
	if err != nil {
		return nil, err
	}
	return fv.Elm, nil
}

func (d *execDecoder) String() string {
	return d.coder.String()
}

// NewCoder infers a Coder for any bound full type.
func NewCoder(t FullType) Coder {
	c, err := inferCoder(t)
	if err != nil {
		panic(err) // for now
	}
	return Coder{c}
}

func inferCoder(t FullType) (*coder.Coder, error) {
	switch t.Class() {
	case typex.Concrete, typex.Container:
		switch t.Type() {
		case reflectx.Int64:
			// use the beam varint coder.
			return &coder.Coder{Kind: coder.VarInt, T: t}, nil
		case reflectx.Int, reflectx.Int8, reflectx.Int16, reflectx.Int32:
			c, err := coderx.NewVarIntZ(t.Type())
			if err != nil {
				return nil, err
			}
			return coder.CoderFrom(c), nil
		case reflectx.Uint, reflectx.Uint8, reflectx.Uint16, reflectx.Uint32, reflectx.Uint64:
			c, err := coderx.NewVarUintZ(t.Type())
			if err != nil {
				return nil, err
			}
			return coder.CoderFrom(c), nil

		case reflectx.Float32:
			c, err := coderx.NewFloat(t.Type())
			if err != nil {
				return nil, err
			}
			return coder.CoderFrom(c), nil

		case reflectx.Float64:
			return &coder.Coder{Kind: coder.Double, T: t}, nil

		case reflectx.String:
			return &coder.Coder{Kind: coder.String, T: t}, nil

		case reflectx.ByteSlice:
			return &coder.Coder{Kind: coder.Bytes, T: t}, nil

		case reflectx.Bool:
			return &coder.Coder{Kind: coder.Bool, T: t}, nil

		default:
			et := t.Type()
			if c := coder.LookupCustomCoder(et); c != nil {
				return coder.CoderFrom(c), nil
			}
			// Interface types that implement JSON marshalling can be handled by the default coder.
			// otherwise, inference needs to fail here.
			if et.Kind() == reflect.Interface && !et.Implements(jsonCoderType) {
				return nil, errors.Errorf("inferCoder failed: interface type %v has no coder registered", et)
			}

			c, err := newJSONCoder(et)
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
			// TODO(herohde) 4/15/2018: do we ever infer W types now that PCollections
			// are non-windowed? We either need to know the windowing strategy or
			// we should remove this case.
			return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: c, Window: coder.NewGlobalWindow()}, nil

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

// protoEnc marshals the supplied proto.Message.
func protoEnc(in T) ([]byte, error) {
	buf := proto.NewBuffer(nil)
	buf.SetDeterministic(true)
	if err := buf.Marshal(in.(proto.Message)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// protoDec unmarshals the supplied bytes into an instance of the supplied
// proto.Message type.
func protoDec(t reflect.Type, in []byte) (T, error) {
	val := reflect.New(t.Elem()).Interface().(proto.Message)
	if err := proto.Unmarshal(in, val); err != nil {
		return nil, err
	}
	return val, nil
}

// Concrete and universal custom coders both have a similar signature.
// Conversion is handled by reflection.

// jsonEnc encodes the supplied value in JSON.
func jsonEnc(in T) ([]byte, error) {
	return json.Marshal(in)
}

// jsonDec decodes the supplied JSON into an instance of the supplied type.
func jsonDec(t reflect.Type, in []byte) (T, error) {
	val := reflect.New(t)
	if err := json.Unmarshal(in, val.Interface()); err != nil {
		return nil, err
	}
	return val.Elem().Interface(), nil
}

func newJSONCoder(t reflect.Type) (*coder.CustomCoder, error) {
	c, err := coder.NewCustomCoder("json", t, jsonEnc, jsonDec)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid coder")
	}
	return c, nil
}
