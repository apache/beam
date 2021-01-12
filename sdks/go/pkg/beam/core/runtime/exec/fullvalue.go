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

package exec

import (
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TODO(herohde) 1/29/2018: using FullValue for nested KVs is somewhat of a hack
// and is errorprone. Consider making it a more explicit data structure.

// FullValue represents the full runtime value for a data element, incl. the
// implicit context. The result of a GBK or CoGBK is not a single FullValue.
// The consumer is responsible for converting the values to the correct type.
// To represent a nested KV with FullValues, assign a *FullValue to Elm/Elm2.
type FullValue struct {
	Elm  interface{} // Element or KV key.
	Elm2 interface{} // KV value, if not invalid

	Timestamp typex.EventTime
	Windows   []typex.Window
}

func (v *FullValue) String() string {
	if v.Elm2 == nil {
		return fmt.Sprintf("%v [@%v:%v]", v.Elm, v.Timestamp, v.Windows)
	}
	return fmt.Sprintf("KV<%v,%v> [@%v:%v]", v.Elm, v.Elm2, v.Timestamp, v.Windows)
}

// Stream is a FullValue reader. It returns io.EOF when complete, but can be
// prematurely closed.
type Stream interface {
	io.Closer
	Read() (*FullValue, error)
}

// ReStream is re-iterable stream, i.e., a Stream factory.
type ReStream interface {
	Open() (Stream, error)
}

// FixedReStream is a simple in-memory ReStream.
type FixedReStream struct {
	Buf []FullValue
}

// Open returns the a Stream from the start of the in-memory ReStream.
func (n *FixedReStream) Open() (Stream, error) {
	return &FixedStream{Buf: n.Buf}, nil
}

// FixedStream is a simple in-memory Stream from a fixed array.
type FixedStream struct {
	Buf  []FullValue
	next int
}

// Close releases the buffer, closing the stream.
func (s *FixedStream) Close() error {
	s.Buf = nil
	return nil
}

// Read produces the next value in the stream.
func (s *FixedStream) Read() (*FullValue, error) {
	if s.Buf == nil || s.next == len(s.Buf) {
		return nil, io.EOF
	}
	ret := s.Buf[s.next]
	s.next++
	return &ret, nil
}

// TODO(herohde) 1/19/2018: type-specialize list and other conversions?

// Convert converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func Convert(v interface{}, to reflect.Type) interface{} {
	from := reflect.TypeOf(v)
	return ConvertFn(from, to)(v)
}

// ConvertFn returns a function that converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func ConvertFn(from, to reflect.Type) func(interface{}) interface{} {
	switch {
	case from == to:
		return identity

	case typex.IsUniversal(from):
		return universal

	case typex.IsList(from) && typex.IsList(to):
		fromE := from.Elem()
		toE := to.Elem()
		cvtFn := ConvertFn(fromE, toE)
		return func(v interface{}) interface{} {
			// Convert []A to []B.
			value := reflect.ValueOf(v)

			ret := reflect.New(to).Elem()
			for i := 0; i < value.Len(); i++ {
				ret = reflect.Append(ret, reflect.ValueOf(cvtFn(value.Index(i).Interface())))
			}
			return ret.Interface()
		}

	case typex.IsList(from) && typex.IsUniversal(from.Elem()) && typex.IsUniversal(to):
		fromE := from.Elem()
		return func(v interface{}) interface{} {
			// Convert []typex.T to the underlying type []T.

			value := reflect.ValueOf(v)
			// We don't know the underlying element type of a nil/empty universal-typed slice.
			// So the best we could do is to return it as is.
			if value.Len() == 0 {
				return v
			}

			toE := reflectx.UnderlyingType(value.Index(0)).Type()
			cvtFn := ConvertFn(fromE, toE)
			ret := reflect.New(reflect.SliceOf(toE)).Elem()
			for i := 0; i < value.Len(); i++ {
				ret = reflect.Append(ret, reflect.ValueOf(cvtFn(value.Index(i).Interface())))
			}
			return ret.Interface()
		}

	default:
		// Arguably this should be:
		//   reflect.ValueOf(v).Convert(to).Interface()
		// but this isn't desirable as it would add avoidable overhead to
		// functions where it applies. A user will have better performance
		// by explicitly doing the type conversion in their code, which
		// the error will indicate. Slow Magic vs Fast & Explicit.
		return identity
	}
}

// identity is the identity function.
func identity(v interface{}) interface{} {
	return v
}

// universal drops the universal type and re-interfaces it to the actual one.
func universal(v interface{}) interface{} {
	return reflectx.UnderlyingType(reflect.ValueOf(v)).Interface()
}

// ReadAll read a full restream and returns the result.
func ReadAll(rs ReStream) ([]FullValue, error) {
	s, err := rs.Open()
	if err != nil {
		return nil, err
	}
	defer s.Close()

	var ret []FullValue
	for {
		elm, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return ret, nil
			}
			return nil, err
		}
		ret = append(ret, *elm)
	}
}
