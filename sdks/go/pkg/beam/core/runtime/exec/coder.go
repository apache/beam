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

	"bytes"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/ioutilx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// NOTE(herohde) 4/30/2017: The main complication is CoGBK results, which have
// nested streams. Hence, a simple read-one-element-at-a-time approach doesn't
// work, because each "element" can be too large to fit into memory. Instead,
// we handle the top GBK/CoGBK layer in the processing node directly.

// ElementEncoder handles FullValue serialization to a byte stream. The encoder
// can be reused, even if an error is encountered. Concurrency-safe.
type ElementEncoder interface {
	// Encode serializes the given value to the writer.
	Encode(*FullValue, io.Writer) error
}

// EncodeElement is a convenience function for encoding a single element into a
// byte slice.
func EncodeElement(c ElementEncoder, val interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := c.Encode(&FullValue{Elm: val}, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ElementDecoder handles FullValue deserialization from a byte stream. The decoder
// can be reused, even if an error is encountered.  Concurrency-safe.
type ElementDecoder interface {
	// Decode deserializes a value from the given reader.
	Decode(io.Reader) (*FullValue, error)
	// DecodeTo deserializes a value from the given reader into the provided FullValue.
	DecodeTo(io.Reader, *FullValue) error
}

// MakeElementEncoder returns a ElementCoder for the given coder. It panics
// if given a coder with stream types, such as GBK.
func MakeElementEncoder(c *coder.Coder) ElementEncoder {
	switch c.Kind {
	case coder.Bytes:
		return &bytesEncoder{}

	case coder.Bool:
		return &boolEncoder{}

	case coder.VarInt:
		return &varIntEncoder{}

	case coder.Double:
		return &doubleEncoder{}

	case coder.String:
		return &stringEncoder{}

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

	case coder.Bool:
		return &boolDecoder{}

	case coder.VarInt:
		return &varIntDecoder{}

	case coder.Double:
		return &doubleDecoder{}

	case coder.String:
		return &stringDecoder{}

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

func (*bytesEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: size (varint) + raw data
	var data []byte
	data, ok := val.Elm.([]byte)
	if !ok {
		return errors.Errorf("received unknown value type: want []byte, got %T", val.Elm)
	}
	size := len(data)

	if err := coder.EncodeVarInt((int64)(size), w); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

type bytesDecoder struct{}

func (*bytesDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: size (varint) + raw data

	size, err := coder.DecodeVarInt(r)
	if err != nil {
		return err
	}
	data, err := ioutilx.ReadN(r, (int)(size))
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: data}
	return nil
}

func (d *bytesDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type boolEncoder struct{}

func (*boolEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: false = 0, true = 1
	var err error
	if val.Elm.(bool) {
		_, err = ioutilx.WriteUnsafe(w, []byte{1})
	} else {
		_, err = ioutilx.WriteUnsafe(w, []byte{0})
	}
	if err != nil {
		return fmt.Errorf("error encoding bool: %v", err)
	}
	return nil
}

type boolDecoder struct{}

func (*boolDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: false = 0, true = 1
	b := make([]byte, 1, 1)
	if err := ioutilx.ReadNBufUnsafe(r, b); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("error decoding bool: %v", err)
	}
	switch b[0] {
	case 0:
		*fv = FullValue{Elm: false}
		return nil
	case 1:
		*fv = FullValue{Elm: true}
		return nil
	}
	return fmt.Errorf("error decoding bool: received invalid value %v", b)
}

func (d *boolDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type varIntEncoder struct{}

func (*varIntEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: beam varint
	return coder.EncodeVarInt(val.Elm.(int64), w)
}

type varIntDecoder struct{}

func (*varIntDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: beam varint
	n, err := coder.DecodeVarInt(r)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: n}
	return nil
}

func (d *varIntDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type doubleEncoder struct{}

func (*doubleEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: beam double (big-endian 64-bit IEEE 754 double)
	return coder.EncodeDouble(val.Elm.(float64), w)
}

type doubleDecoder struct{}

func (*doubleDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: beam double (big-endian 64-bit IEEE 754 double)
	f, err := coder.DecodeDouble(r)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: f}
	return nil
}

func (d *doubleDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type stringEncoder struct{}

func (*stringEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: beam utf8 string (length prefix + run of bytes)
	return coder.EncodeStringUTF8(val.Elm.(string), w)
}

type stringDecoder struct{}

func (*stringDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: beam utf8 string (length prefix + run of bytes)
	f, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: f}
	return nil
}

func (d *stringDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type customEncoder struct {
	t   reflect.Type
	enc Encoder
}

func (c *customEncoder) Encode(val *FullValue, w io.Writer) error {
	// (1) Call encode

	data, err := c.enc.Encode(c.t, val.Elm)
	if err != nil {
		return err
	}

	// (2) Add length prefix

	size := len(data)
	if err := coder.EncodeVarInt((int64)(size), w); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

type customDecoder struct {
	t   reflect.Type
	dec Decoder
}

func (c *customDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// (1) Read length-prefixed encoded data

	size, err := coder.DecodeVarInt(r)
	if err != nil {
		return err
	}
	data, err := ioutilx.ReadN(r, (int)(size))
	if err != nil {
		return err
	}

	// (2) Call decode

	val, err := c.dec.Decode(c.t, data)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: val}
	return nil
}

func (c *customDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := c.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type kvEncoder struct {
	fst, snd ElementEncoder
	cached   FullValue
}

func (c *kvEncoder) Encode(val *FullValue, w io.Writer) error {
	defer func() {
		// clear the cached FullValue after use to avoid leaks.
		c.cached = FullValue{}
	}()
	if err := c.fst.Encode(convertIfNeeded(val.Elm, &c.cached), w); err != nil {
		return err
	}
	return c.snd.Encode(convertIfNeeded(val.Elm2, &c.cached), w)
}

type kvDecoder struct {
	fst, snd ElementDecoder
}

func (c *kvDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	var key FullValue
	if err := c.fst.DecodeTo(r, &key); err != nil {
		return err
	}
	var value FullValue
	if err := c.snd.DecodeTo(r, &value); err != nil {
		return err
	}
	*fv = FullValue{Elm: elideSingleElmFV(&key), Elm2: elideSingleElmFV(&value)}
	return nil
}

// Decode returns a *FullValue containing the contents of the decoded KV. If
// one of the elements of the KV is a nested KV, then the corresponding Elm
// field in the returned value will be another *FullValue. Otherwise, the
// Elm will be the decoded type.
//
// Example:
//   KV<int, KV<...>> decodes to *FullValue{Elm: int, Elm2: *FullValue{...}}
func (c *kvDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := c.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

// elideSingleElmFV elides a FullValue if it has only one element, returning
// the contents of the first element, but returning the FullValue unchanged
// if it has two elements.
//
// Technically drops window and timestamp info, so only use when those are
// expected to be empty.
func elideSingleElmFV(fv *FullValue) interface{} {
	if fv.Elm2 == nil {
		return fv.Elm
	}
	return fv
}

// convertIfNeeded reuses Wrapped KVs if needed, but accepts pointer
// to a pre-allocated non-nil *FullValue for overwriting and use.
func convertIfNeeded(v interface{}, allocated *FullValue) *FullValue {
	if fv, ok := v.(*FullValue); ok {
		return fv
	} else if _, ok := v.(FullValue); ok {
		panic("Nested FullValues must be nested as pointers.")
	}
	*allocated = FullValue{Elm: v}
	return allocated
}

// WindowEncoder handles Window serialization to a byte stream. The encoder
// can be reused, even if an error is encountered. Concurrency-safe.
type WindowEncoder interface {
	// Encode serializes the given value to the writer.
	Encode([]typex.Window, io.Writer) error
	EncodeSingle(typex.Window, io.Writer) error
}

// EncodeWindow is a convenience function for encoding a single window into a
// byte slice.
func EncodeWindow(c WindowEncoder, w typex.Window) ([]byte, error) {
	var buf bytes.Buffer
	if err := c.EncodeSingle(w, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// WindowDecoder handles Window deserialization from a byte stream. The decoder
// can be reused, even if an error is encountered. Concurrency-safe.
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

func (*globalWindowEncoder) EncodeSingle(ws typex.Window, w io.Writer) error {
	return nil
}

type globalWindowDecoder struct{}

func (*globalWindowDecoder) Decode(r io.Reader) ([]typex.Window, error) {
	_, err := coder.DecodeInt32(r) // #windows
	return window.SingleGlobalWindow, err
}

type intervalWindowEncoder struct{}

func (enc *intervalWindowEncoder) Encode(ws []typex.Window, w io.Writer) error {
	if err := coder.EncodeInt32(int32(len(ws)), w); err != nil { // #windows
		return err
	}
	for _, elm := range ws {
		if err := enc.EncodeSingle(elm, w); err != nil {
			return nil
		}
	}
	return nil
}

func (*intervalWindowEncoder) EncodeSingle(elm typex.Window, w io.Writer) error {
	// Encoding: upper bound and duration
	iw := elm.(window.IntervalWindow)
	if err := coder.EncodeEventTime(iw.End, w); err != nil {
		return err
	}
	duration := iw.End.Milliseconds() - iw.Start.Milliseconds()
	if err := coder.EncodeVarUint64(uint64(duration), w); err != nil {
		return err
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

var paneNoFiring = []byte{0xf}

// EncodeWindowedValueHeader serializes a windowed value header.
func EncodeWindowedValueHeader(enc WindowEncoder, ws []typex.Window, t typex.EventTime, w io.Writer) error {
	// Encoding: Timestamp, Window, Pane (header) + Element

	if err := coder.EncodeEventTime(t, w); err != nil {
		return err
	}
	if err := enc.Encode(ws, w); err != nil {
		return err
	}
	_, err := w.Write(paneNoFiring)
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
	var data [1]byte
	if err := ioutilx.ReadNBufUnsafe(r, data[:]); err != nil { // NO_FIRING pane
		return nil, mtime.ZeroTimestamp, err
	}
	return ws, t, nil
}
