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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Port represents the connection port of external operations.
type Port struct {
	URL string
}

// Target represents the target of external operations.
type Target struct {
	ID   string
	Name string
}

// StreamID represents the information needed to identify a data stream.
type StreamID struct {
	Port   Port
	Target Target
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

// ElementEncoder handles FullValue serialization to a byte stream. The encoder
// can be reused, even if an error is encountered.
type ElementEncoder interface {
	// Encode serializes the given value to the writer.
	Encode(FullValue, io.Writer) error
}

// ElementDecoder handles FullValue deserialization from a byte stream. The decoder
// can be reused, even if an error is encountered.
type ElementDecoder interface {
	// Decode deserializes a value from the given reader.
	Decode(io.Reader) (FullValue, error)
}

// MakeElementEncoder returns a ElementCoder for the given coder. It panics
// if given a coder with stream types, such as GBK.
func MakeElementEncoder(c *coder.Coder) ElementEncoder {
	switch c.Kind {
	case coder.Bytes:
		return &bytesEncoder{}

	case coder.VarInt:
		return &varIntEncoder{}

	case coder.Custom:
		return &customEncoder{
			t:   c.Custom.Type,
			enc: makeEncoder(c.Custom.Enc.Fn),
		}

	case coder.KV:
		return &kvEncoder{
			fst: MakeElementEncoder(c.Components[0]),
			snd: MakeElementEncoder(c.Components[1]),
		}

	default:
		panic(fmt.Sprintf("Unexpected coder: %v", c))
	}
}

// MakeElementDecoder returns a ElementDecoder for the given coder. It panics
// if given a coder with stream types, such as GBK.
func MakeElementDecoder(c *coder.Coder) ElementDecoder {
	switch c.Kind {
	case coder.Bytes:
		return &bytesDecoder{}

	case coder.VarInt:
		return &varIntDecoder{}

	case coder.Custom:
		return &customDecoder{
			t:   c.Custom.Type,
			dec: makeDecoder(c.Custom.Dec.Fn),
		}

	case coder.KV:
		return &kvDecoder{
			fst: MakeElementDecoder(c.Components[0]),
			snd: MakeElementDecoder(c.Components[1]),
		}

	default:
		panic(fmt.Sprintf("Unexpected coder: %v", c))
	}
}

type bytesEncoder struct{}

func (*bytesEncoder) Encode(val FullValue, w io.Writer) error {
	// Encoding: size (varint) + raw data
	var data []byte
	switch v := val.Elm.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("received unknown value type: want []byte or string, got %T", v)
	}
	size := len(data)

	if err := coder.EncodeVarInt((int32)(size), w); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

type bytesDecoder struct{}

func (*bytesDecoder) Decode(r io.Reader) (FullValue, error) {
	// Encoding: size (varint) + raw data

	size, err := coder.DecodeVarInt(r)
	if err != nil {
		return FullValue{}, err
	}
	data, err := ioutilx.ReadN(r, (int)(size))
	if err != nil {
		return FullValue{}, err
	}
	return FullValue{Elm: data}, nil
}

type varIntEncoder struct{}

func (*varIntEncoder) Encode(val FullValue, w io.Writer) error {
	// Encoding: beam varint

	n := Convert(val.Elm, reflectx.Int32).(int32) // Convert needed?
	return coder.EncodeVarInt(n, w)
}

type varIntDecoder struct{}

func (*varIntDecoder) Decode(r io.Reader) (FullValue, error) {
	// Encoding: beam varint

	n, err := coder.DecodeVarInt(r)
	if err != nil {
		return FullValue{}, err
	}
	return FullValue{Elm: n}, nil
}

type customEncoder struct {
	t   reflect.Type
	enc Encoder
}

func (c *customEncoder) Encode(val FullValue, w io.Writer) error {
	// (1) Call encode

	data, err := c.enc.Encode(c.t, val.Elm)
	if err != nil {
		return err
	}

	// (2) Add length prefix

	size := len(data)
	if err := coder.EncodeVarInt((int32)(size), w); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

type customDecoder struct {
	t   reflect.Type
	dec Decoder
}

func (c *customDecoder) Decode(r io.Reader) (FullValue, error) {
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

	val, err := c.dec.Decode(c.t, data)
	if err != nil {
		return FullValue{}, err
	}
	return FullValue{Elm: val}, err
}

type kvEncoder struct {
	fst, snd ElementEncoder
}

func (c *kvEncoder) Encode(val FullValue, w io.Writer) error {
	if err := c.fst.Encode(convertIfNeeded(val.Elm), w); err != nil {
		return err
	}
	return c.snd.Encode(convertIfNeeded(val.Elm2), w)
}

type kvDecoder struct {
	fst, snd ElementDecoder
}

func (c *kvDecoder) Decode(r io.Reader) (FullValue, error) {
	key, err := c.fst.Decode(r)
	if err != nil {
		return FullValue{}, err
	}
	value, err := c.snd.Decode(r)
	if err != nil {
		return FullValue{}, err
	}
	return FullValue{Elm: key.Elm, Elm2: value.Elm}, nil

}

// WindowEncoder handles Window serialization to a byte stream. The encoder
// can be reused, even if an error is encountered.
type WindowEncoder interface {
	// Encode serializes the given value to the writer.
	Encode([]typex.Window, io.Writer) error
}

// WindowDecoder handles Window deserialization from a byte stream. The decoder
// can be reused, even if an error is encountered.
type WindowDecoder interface {
	// Decode deserializes a value from the given reader.
	Decode(io.Reader) ([]typex.Window, error)
}

// MakeWindowEncoder returns a WindowEncoder for the given window coder.
func MakeWindowEncoder(c *coder.WindowCoder) WindowEncoder {
	switch c.Kind {
	case coder.GlobalWindow:
		return &globalWindowEncoder{}

	case coder.IntervalWindow:
		return &intervalWindowEncoder{}

	default:
		panic(fmt.Sprintf("Unexpected window coder: %v", c))
	}
}

// MakeWindowDecoder returns a WindowDecoder for the given window coder.
func MakeWindowDecoder(c *coder.WindowCoder) WindowDecoder {
	switch c.Kind {
	case coder.GlobalWindow:
		return &globalWindowDecoder{}

	case coder.IntervalWindow:
		return &intervalWindowDecoder{}

	default:
		panic(fmt.Sprintf("Unexpected window coder: %v", c))
	}
}

type globalWindowEncoder struct{}

func (*globalWindowEncoder) Encode(ws []typex.Window, w io.Writer) error {
	// GlobalWindow encodes into the empty string.
	return coder.EncodeInt32(1, w) // #windows
}

type globalWindowDecoder struct{}

func (*globalWindowDecoder) Decode(r io.Reader) ([]typex.Window, error) {
	_, err := coder.DecodeInt32(r) // #windows
	return window.SingleGlobalWindow, err
}

type intervalWindowEncoder struct{}

func (*intervalWindowEncoder) Encode(ws []typex.Window, w io.Writer) error {
	// Encoding: upper bound and duration

	if err := coder.EncodeInt32(int32(len(ws)), w); err != nil { // #windows
		return err
	}
	for _, elm := range ws {
		iw := elm.(window.IntervalWindow)
		if err := coder.EncodeEventTime(iw.End, w); err != nil {
			return err
		}
		duration := iw.End.Milliseconds() - iw.Start.Milliseconds()
		if err := coder.EncodeVarUint64(uint64(duration), w); err != nil {
			return err
		}
	}
	return nil
}

type intervalWindowDecoder struct{}

func (*intervalWindowDecoder) Decode(r io.Reader) ([]typex.Window, error) {
	// Encoding: upper bound and duration

	n, err := coder.DecodeInt32(r) // #windows

	ret := make([]typex.Window, n, n)
	for i := int32(0); i < n; i++ {
		end, err := coder.DecodeEventTime(r)
		if err != nil {
			return nil, err
		}
		duration, err := coder.DecodeVarUint64(r)
		if err != nil {
			return nil, err
		}
		ret[i] = window.IntervalWindow{Start: mtime.FromMilliseconds(end.Milliseconds() - int64(duration)), End: end}
	}
	return ret, err
}

// EncodeWindowedValueHeader serializes a windowed value header.
func EncodeWindowedValueHeader(enc WindowEncoder, ws []typex.Window, t typex.EventTime, w io.Writer) error {
	// Encoding: Timestamp, Window, Pane (header) + Element

	if err := coder.EncodeEventTime(t, w); err != nil {
		return err
	}
	if err := enc.Encode(ws, w); err != nil {
		return err
	}
	_, err := w.Write([]byte{0xf}) // NO_FIRING pane
	return err
}

// DecodeWindowedValueHeader deserializes a windowed value header.
func DecodeWindowedValueHeader(dec WindowDecoder, r io.Reader) ([]typex.Window, typex.EventTime, error) {
	// Encoding: Timestamp, Window, Pane (header) + Element

	t, err := coder.DecodeEventTime(r)
	if err != nil {
		return nil, mtime.ZeroTimestamp, err
	}
	ws, err := dec.Decode(r)
	if err != nil {
		return nil, mtime.ZeroTimestamp, err
	}
	if _, err := ioutilx.ReadN(r, 1); err != nil { // NO_FIRING pane
		return nil, mtime.ZeroTimestamp, err
	}
	return ws, t, nil
}

func convertIfNeeded(v interface{}) FullValue {
	if fv, ok := v.(FullValue); ok {
		return fv
	}
	return FullValue{Elm: v}
}
