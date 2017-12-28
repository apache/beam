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
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// StreamID represents the information needed to identify a data stream.
type StreamID struct {
	Port   graph.Port
	Target graph.Target
	InstID string
}

func (id StreamID) String() string {
	return fmt.Sprintf("S:%v:[%v:%v]:%v", id.Port.URL, id.Target.ID, id.Target.Name, id.InstID)
}

// DataReader is the interface for reading data elements from a particular stream.
type DataReader interface {
	OpenRead(ctx context.Context, id StreamID) (io.ReadCloser, error)
}

// DataWriter is the interface for writing data elements to a particular stream.
type DataWriter interface {
	OpenWrite(ctx context.Context, id StreamID) (io.WriteCloser, error)
}

// DataManager manages external data byte streams. Each data stream can be
// opened by one consumer only.
type DataManager interface {
	DataReader
	DataWriter
}

// NOTE(herohde) 4/30/2017: The main complication is CoGBK results, which have
// nested streams. Hence, a simple read-one-element-at-a-time approach doesn't
// work, because each "element" can be too large to fit into memory. Instead,
// we handle the top GBK/CoGBK layer in the processing node directly.

// EncodeElement uses the supplied coder to write the data in val to the supplied io.Writer.
func EncodeElement(c *coder.Coder, val FullValue, w io.Writer) error {
	switch c.Kind {
	case coder.Bytes:
		// Encoding: size (varint) + raw data

		data := reflectx.UnderlyingType(val.Elm).Convert(reflectx.ByteSlice).Interface().([]byte)
		size := len(data)

		if err := coder.EncodeVarInt((int32)(size), w); err != nil {
			return err
		}
		_, err := w.Write(data)
		return err

	case coder.VarInt:
		// Encoding: beam varint

		n := reflectx.UnderlyingType(val.Elm).Convert(reflectx.Int32).Interface().(int32)
		return coder.EncodeVarInt(n, w)

	case coder.Custom:
		enc := c.Custom.Enc

		// (1) Call encode

		args := make([]reflect.Value, len(enc.Param))
		if index, ok := enc.Type(); ok {
			args[index] = reflect.ValueOf(c.Custom.Type)
		}
		params := enc.Params(funcx.FnValue)
		args[params[0]] = val.Elm

		ret, err := reflectCallNoPanic(enc.Fn, args)
		if err != nil {
			return err
		}
		if index, ok := enc.Error(); ok && !ret[index].IsNil() {
			return fmt.Errorf("encode error: %v", ret[index].Interface())
		}
		data := ret[enc.Returns(funcx.RetValue)[0]].Interface().([]byte)

		// (2) Add length prefix

		size := len(data)
		if err := coder.EncodeVarInt((int32)(size), w); err != nil {
			return err
		}
		_, err = w.Write(data)
		return err

	case coder.KV:
		if err := EncodeElement(c.Components[0], FullValue{Elm: val.Elm}, w); err != nil {
			return err
		}
		return EncodeElement(c.Components[1], FullValue{Elm: val.Elm2}, w)

	default:
		return fmt.Errorf("unexpected coder: %v", c)
	}
}

// DecodeElement uses the supplied coder to read data from the supplied Reader and return a data element.
func DecodeElement(c *coder.Coder, r io.Reader) (FullValue, error) {
	switch c.Kind {
	case coder.Bytes:
		// Encoding: size (varint) + raw data

		size, err := coder.DecodeVarInt(r)
		if err != nil {
			return FullValue{}, err
		}
		data, err := ioutilx.ReadN(r, (int)(size))
		if err != nil {
			return FullValue{}, err
		}
		return FullValue{Elm: reflect.ValueOf(data)}, nil

	case coder.VarInt:
		// Encoding: beam varint

		n, err := coder.DecodeVarInt(r)
		if err != nil {
			return FullValue{}, err
		}
		return FullValue{Elm: reflect.ValueOf(n)}, nil

	case coder.Custom:
		dec := c.Custom.Dec

		// (1) Read length-prefixed encoded data

		size, err := coder.DecodeVarInt(r)
		if err != nil {
			return FullValue{}, err
		}
		data, err := ioutilx.ReadN(r, (int)(size))
		if err != nil {
			return FullValue{}, err
		}

		// (2) Call decode

		args := make([]reflect.Value, len(dec.Param))
		if index, ok := dec.Type(); ok {
			args[index] = reflect.ValueOf(c.Custom.Type)
		}
		params := dec.Params(funcx.FnValue)
		args[params[0]] = reflect.ValueOf(data)

		ret, err := reflectCallNoPanic(dec.Fn, args)
		if err != nil {
			return FullValue{}, err
		}
		if index, ok := dec.Error(); ok && !ret[index].IsNil() {
			return FullValue{}, fmt.Errorf("decode error: %v", ret[index].Interface())
		}
		return FullValue{Elm: ret[dec.Returns(funcx.RetValue)[0]]}, err

	case coder.KV:
		key, err := DecodeElement(c.Components[0], r)
		if err != nil {
			return FullValue{}, err
		}
		value, err := DecodeElement(c.Components[1], r)
		if err != nil {
			return FullValue{}, err
		}
		return FullValue{Elm: key.Elm, Elm2: value.Elm}, nil

	default:
		return FullValue{}, fmt.Errorf("unexpected coder: %v", c)
	}
}

// TODO(herohde) 4/7/2017: actually handle windows.

// EncodeWindowedValueHeader uses the supplied coder to serialize a windowed value header.
func EncodeWindowedValueHeader(c *coder.Coder, t typex.EventTime, w io.Writer) error {
	// Encoding: Timestamp, Window, Pane (header) + Element

	if err := coder.EncodeEventTime(t, w); err != nil {
		return err
	}
	if err := coder.EncodeInt32(1, w); err != nil { // #windows
		return err
	}
	// Ignore GlobalWindow, for now. It encoded into the empty string.

	_, err := w.Write([]byte{0xf}) // NO_FIRING pane
	return err
}

// DecodeWindowedValueHeader uses the supplied coder to deserialize a windowed value header.
func DecodeWindowedValueHeader(c *coder.Coder, r io.Reader) (typex.EventTime, error) {
	// Encoding: Timestamp, Window, Pane (header) + Element

	t, err := coder.DecodeEventTime(r)
	if err != nil {
		return typex.EventTime(time.Time{}), err
	}
	if _, err := coder.DecodeInt32(r); err != nil { // #windows
		return typex.EventTime(time.Time{}), err
	}
	if _, err := ioutilx.ReadN(r, 1); err != nil { // NO_FIRING pane
		return typex.EventTime(time.Time{}), err
	}
	return t, nil
}
