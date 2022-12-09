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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var (
	encodedTypeType    = reflect.TypeOf((*EncodedType)(nil)).Elem()
	encodedFuncType    = reflect.TypeOf((*EncodedFunc)(nil)).Elem()
	encodedCoderType   = reflect.TypeOf((*EncodedCoder)(nil)).Elem()
	encodedStorageType = reflect.TypeOf((*struct{ EncodedBeamData string })(nil)).Elem()
	timeType           = reflect.TypeOf((*time.Time)(nil)).Elem()
)

func init() {
	RegisterType(encodedTypeType)
	RegisterType(encodedFuncType)
	RegisterType(encodedCoderType)
	RegisterType(timeType)
	schema.RegisterLogicalType(schema.ToLogicalType("beam.EncodedType", encodedTypeType, encodedStorageType))
	schema.RegisterLogicalType(schema.ToLogicalType("beam.EncodedFunc", encodedFuncType, encodedStorageType))
	schema.RegisterLogicalType(schema.ToLogicalType("beam.EncodedCoder", encodedCoderType, encodedStorageType))
	coder.RegisterSchemaProviders(encodedTypeType, encodedTypeEnc, encodedTypeDec)
	coder.RegisterSchemaProviders(encodedFuncType, encodedFuncEnc, encodedFuncDec)
	coder.RegisterSchemaProviders(encodedCoderType, encodedCoderEnc, encodedCoderDec)

	schema.RegisterLogicalType(schema.ToLogicalType("time.Time", timeType, encodedStorageType))
	coder.RegisterSchemaProviders(timeType, timeEnc, timeDec)
}

// EncodedType is a serialization wrapper around a type for convenience.
type EncodedType struct {
	// T is the type to preserve across serialization.
	T reflect.Type
}

// MarshalJSON returns the JSON encoding this value.
func (w EncodedType) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeType(w.T)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedType) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	t, err := graphx.DecodeType(s)
	if err != nil {
		return err
	}
	w.T = t
	return nil
}

func encodedTypeEnc(reflect.Type) (func(any, io.Writer) error, error) {
	return func(iface any, w io.Writer) error {
			if err := coder.WriteSimpleRowHeader(1, w); err != nil {
				return err
			}
			v := iface.(EncodedType)
			str, err := graphx.EncodeType(v.T)
			if err != nil {
				return err
			}
			if err := coder.EncodeStringUTF8(str, w); err != nil {
				return err
			}
			return nil
		},
		nil
}

func encodedTypeDec(reflect.Type) (func(io.Reader) (any, error), error) {
	return func(r io.Reader) (any, error) {
			if err := coder.ReadSimpleRowHeader(1, r); err != nil {
				return nil, err
			}
			s, err := coder.DecodeStringUTF8(r)
			if err != nil {
				return nil, err
			}
			t, err := graphx.DecodeType(s)
			if err != nil {
				return nil, err
			}
			return EncodedType{
				T: t,
			}, nil
		},
		nil
}

// EncodedFunc is a serialization wrapper around a function for convenience.
type EncodedFunc struct {
	// Fn is the function to preserve across serialization.
	Fn reflectx.Func
}

// MarshalJSON returns the JSON encoding this value.
func (w EncodedFunc) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeFn(w.Fn)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedFunc) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	fn, err := graphx.DecodeFn(s)
	if err != nil {
		return err
	}
	w.Fn = fn
	return nil
}

func encodedFuncEnc(reflect.Type) (func(any, io.Writer) error, error) {
	return func(iface any, w io.Writer) error {
			if err := coder.WriteSimpleRowHeader(1, w); err != nil {
				return err
			}
			v := iface.(EncodedFunc)
			str, err := graphx.EncodeFn(v.Fn)
			if err != nil {
				return err
			}
			if err := coder.EncodeStringUTF8(str, w); err != nil {
				return err
			}
			return nil
		},
		nil
}

func encodedFuncDec(reflect.Type) (func(io.Reader) (any, error), error) {
	return func(r io.Reader) (any, error) {
			if err := coder.ReadSimpleRowHeader(1, r); err != nil {
				return nil, err
			}
			s, err := coder.DecodeStringUTF8(r)
			if err != nil {
				return nil, err
			}
			fn, err := graphx.DecodeFn(s)
			if err != nil {
				return nil, err
			}
			return EncodedFunc{
				Fn: fn,
			}, nil
		},
		nil
}

// DecodeCoder decodes a coder. Any custom coder function symbol must be
// resolvable via the runtime.GlobalSymbolResolver. The types must be encodable.
func DecodeCoder(data string) (Coder, error) {
	c, err := graphx.DecodeCoder(data)
	if err != nil {
		return Coder{}, err
	}
	return Coder{coder: c}, nil
}

// EncodedCoder is a serialization wrapper around a coder for convenience.
type EncodedCoder struct {
	// Coder is the coder to preserve across serialization.
	Coder Coder
}

// MarshalJSON returns the JSON encoding this value.
func (w EncodedCoder) MarshalJSON() ([]byte, error) {
	str, err := graphx.EncodeCoder(w.Coder.coder)
	if err != nil {
		return nil, err
	}
	return json.Marshal(str)
}

// UnmarshalJSON sets the state of this instance from the passed in JSON.
func (w *EncodedCoder) UnmarshalJSON(buf []byte) error {
	var s string
	if err := json.Unmarshal(buf, &s); err != nil {
		return err
	}
	c, err := graphx.DecodeCoder(s)
	if err != nil {
		return err
	}
	w.Coder = Coder{coder: c}
	return nil
}

func encodedCoderEnc(reflect.Type) (func(any, io.Writer) error, error) {
	return func(iface any, w io.Writer) error {
			if err := coder.WriteSimpleRowHeader(1, w); err != nil {
				return err
			}
			v := iface.(EncodedCoder)
			str, err := graphx.EncodeCoder(v.Coder.coder)
			if err != nil {
				return err
			}
			if err := coder.EncodeStringUTF8(str, w); err != nil {
				return err
			}
			return nil
		},
		nil
}

func encodedCoderDec(reflect.Type) (func(io.Reader) (any, error), error) {
	return func(r io.Reader) (any, error) {
			if err := coder.ReadSimpleRowHeader(1, r); err != nil {
				return nil, err
			}
			s, err := coder.DecodeStringUTF8(r)
			if err != nil {
				return nil, err
			}
			c, err := graphx.DecodeCoder(s)
			if err != nil {
				return EncodedCoder{}, err
			}
			return EncodedCoder{Coder: Coder{coder: c}}, nil
		},
		nil
}

func timeEnc(reflect.Type) (func(any, io.Writer) error, error) {
	return func(iface any, w io.Writer) error {
		if err := coder.WriteSimpleRowHeader(1, w); err != nil {
			return errors.Wrap(err, "encoding time.Time schema override")
		}
		t := iface.(time.Time)
		// We use the text marshalling rather than the binary marshalling
		// since it has more precision. Apparently some info isn't included
		// in the binary marshal.
		data, err := t.MarshalText()
		if err != nil {
			return fmt.Errorf("marshalling time: %v", err)
		}
		if err := coder.EncodeBytes(data, w); err != nil {
			return err
		}
		return nil
	}, nil
}

func timeDec(reflect.Type) (func(io.Reader) (any, error), error) {
	return func(r io.Reader) (any, error) {
		if err := coder.ReadSimpleRowHeader(1, r); err != nil {
			return nil, errors.Wrap(err, "decoding time.Time schema override")
		}
		data, err := coder.DecodeBytes(r)
		if err != nil {
			return nil, errors.Wrap(err, "retrieving time data: %v")
		}
		t := time.Time{}
		if err := t.UnmarshalText(data); err != nil {
			return nil, errors.Wrap(err, "decoding time: %v")
		}
		return t, nil
	}, nil
}
