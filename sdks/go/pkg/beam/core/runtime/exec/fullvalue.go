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

// FullValue represents the full runtime value for a data element, incl. the
// implicit context. The result of a GBK or CoGBK is not a single FullValue.
// The consumer is responsible for converting the values to the correct type.
type FullValue struct {
	Elm  reflect.Value // Element or KV key.
	Elm2 reflect.Value // KV value, if not invalid

	Timestamp typex.EventTime
	// TODO: Window, pane
}

func (v FullValue) String() string {
	if v.Elm2.Kind() == reflect.Invalid {
		return fmt.Sprintf("%v [@%v]", v.Elm.Interface(), time.Time(v.Timestamp).Unix())
	}
	return fmt.Sprintf("KV<%v,%v> [@%v]", v.Elm.Interface(), v.Elm2.Interface(), time.Time(v.Timestamp).Unix())
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

// Convert converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func Convert(value reflect.Value, to reflect.Type) reflect.Value {
	from := value.Type()
	switch {
	case from == to:
		return value

	case typex.IsUniversal(from):
		// We need to drop T to obtain the underlying type of the value.
		return reflectx.UnderlyingType(value)

	case typex.IsList(from) && typex.IsList(to):
		// Convert []A to []B.

		ret := reflect.New(to).Elem()
		for i := 0; i < value.Len(); i++ {
			ret = reflect.Append(ret, Convert(value.Index(i), to.Elem()))
		}
		return ret

	default:
		switch {
		// Perform conservative type conversions.
		case from == reflectx.ByteSlice && to == reflectx.String:
			return value.Convert(to)

		default:
			return value
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
