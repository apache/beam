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
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TODO(herohde) 1/29/2018: using FullValue for nested KVs is somewhat of a hack
// and is errorprone. Consider making it a more explicit data structure.

// FullValue represents the full runtime value for a data element, incl. the
// implicit context. The result of a GBK or CoGBK is not a single FullValue.
// The consumer is responsible for converting the values to the correct type.
type FullValue struct {
	Elm  interface{} // Element or KV key.
	Elm2 interface{} // KV value, if not invalid

	Timestamp typex.EventTime
	// TODO: Window, pane
}

func (v FullValue) String() string {

	if v.Elm2 == nil {
		return fmt.Sprintf("%v [@%v]", v.Elm, time.Time(v.Timestamp).Unix())
	}
	return fmt.Sprintf("KV<%v,%v> [@%v]", v.Elm, v.Elm2, time.Time(v.Timestamp).Unix())
}

// Stream is a FullValue reader. It returns io.EOF when complete, but can be
// prematurely closed.
type Stream interface {
	io.Closer
	Read() (FullValue, error)
}

// ReStream is Stream factory.
type ReStream interface {
	Open() Stream
}

// FixedReStream is a simple in-memory ReSteam.
type FixedReStream struct {
	Buf []FullValue
}

func (n *FixedReStream) Open() Stream {
	return &FixedStream{Buf: n.Buf}
}

// FixedStream is a simple in-memory Stream from a fixed array.
type FixedStream struct {
	Buf  []FullValue
	next int
}

func (s *FixedStream) Close() error {
	s.Buf = nil
	return nil
}

func (s *FixedStream) Read() (FullValue, error) {
	if s.Buf == nil || s.next == len(s.Buf) {
		return FullValue{}, io.EOF
	}
	ret := s.Buf[s.next]
	s.next++
	return ret, nil
}

// TODO(herohde) 1/19/2018: type-specialize list and other conversions?

// Convert converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func Convert(v interface{}, to reflect.Type) interface{} {
	from := reflect.TypeOf(v)

	switch {
	case from == to:
		return v

	case typex.IsUniversal(from):
		// We need to drop T to obtain the underlying type of the value.
		return reflectx.UnderlyingType(reflect.ValueOf(v)).Interface()
		// TODO(herohde) 1/19/2018: reflect.ValueOf(v).Convert(to).Interface() instead?

	case typex.IsList(from) && typex.IsList(to):
		// Convert []A to []B.

		value := reflect.ValueOf(v)

		ret := reflect.New(to).Elem()
		for i := 0; i < value.Len(); i++ {
			ret = reflect.Append(ret, reflect.ValueOf(Convert(value.Index(i).Interface(), to.Elem())))
		}
		return ret.Interface()

	default:
		switch {
		// Perform conservative type conversions.
		case from == reflectx.ByteSlice && to == reflectx.String:
			return string(v.([]byte))

		default:
			return v
		}
	}
}

// ReadAll read the full stream and returns the result. It always closes the stream.
func ReadAll(s Stream) ([]FullValue, error) {
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
		ret = append(ret, elm)
	}
}
