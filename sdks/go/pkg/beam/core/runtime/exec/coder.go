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
	"strings"

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
// can be reused, even if an error is encountered.
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
// can be reused, even if an error is encountered.
type ElementDecoder interface {
	// Decode deserializes a value from the given reader.
	Decode(io.Reader) (*FullValue, error)
	// DecodeTo deserializes a value from the given reader into the provided FullValue.
	DecodeTo(io.Reader, *FullValue) error
}

// MakeElementEncoder returns a ElementCoder for the given coder.
// It panics if given an unknown coder, or a coder with stream types, such as GBK.
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
		enc := &customEncoder{
			t:   c.Custom.Type,
			enc: makeEncoder(c.Custom.Enc.Fn),
		}
		if c.Custom.Name != "schema" {
			return enc
		}
		// Custom schema coding is shorthand for using beam infrastructure
		// wrapped in a custom coder.
		switch c.T.Type().Kind() {
		case reflect.Slice, reflect.Array:
			// We don't permit registering custom coders for slice types
			// so we must length prefix the entire iterable.
			return &lpEncoder{
				enc: &iterableEncoder{
					t:   c.Custom.Type,
					enc: enc,
				},
			}
		}
		return enc

	case coder.LP:
		return &lpEncoder{
			enc: MakeElementEncoder(c.Components[0]),
		}

	case coder.KV:
		return &kvEncoder{
			fst: MakeElementEncoder(c.Components[0]),
			snd: MakeElementEncoder(c.Components[1]),
		}

	case coder.Window:
		return &wrappedWindowEncoder{
			enc: MakeWindowEncoder(c.Window),
		}

	case coder.Iterable:
		return &iterableEncoder{
			enc: MakeElementEncoder(c.Components[0]),
		}

	case coder.WindowedValue:
		return &windowedValueEncoder{
			elm: MakeElementEncoder(c.Components[0]),
			win: MakeWindowEncoder(c.Window),
		}

	case coder.ParamWindowedValue:
		return &paramWindowedValueEncoder{
			elm: MakeElementEncoder(c.Components[0]),
			win: MakeWindowEncoder(c.Window),
		}

	case coder.Timer:
		return &timerEncoder{
			elm: MakeElementEncoder(c.Components[0]),
		}

	case coder.Row:
		enc, err := coder.RowEncoderForStruct(c.T.Type())
		if err != nil {
			panic(err)
		}
		return &rowEncoder{
			enc: enc,
		}

	default:
		panic(fmt.Sprintf("Unexpected coder: %v", c))
	}
}

// MakeElementDecoder returns a ElementDecoder for the given coder.
// It panics if given an unknown coder, or a coder with stream types, such as GBK.
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
		dec := &customDecoder{
			t:   c.Custom.Type,
			dec: makeDecoder(c.Custom.Dec.Fn),
		}

		fmt.Println("getting decoder", c.Custom)
		if c.Custom.Name != "schema" {
			return dec
		}
		// Custom schema coding is shorthand for using beam infrastructure
		// wrapped in a custom coder.
		switch c.T.Type().Kind() {
		case reflect.Slice:
			return &lpDecoder{
				dec: &iterableDecoder{
					t:   c.Custom.Type,
					dec: dec,
				},
			}
		case reflect.Array:
			return &lpDecoder{
				dec: &arrayDecoder{
					t:   c.Custom.Type,
					dec: dec,
				},
			}
		}
		return dec

	case coder.LP:
		return &lpDecoder{
			dec: MakeElementDecoder(c.Components[0]),
		}

	case coder.KV:
		return &kvDecoder{
			fst: MakeElementDecoder(c.Components[0]),
			snd: MakeElementDecoder(c.Components[1]),
		}

	// The following cases are not expected to be executed in the normal
	// course of a pipeline, however including them here enables simpler
	// end to end validation of standard coders against
	// the standard_coders.yaml specs.
	case coder.Window:
		return &wrappedWindowDecoder{
			dec: MakeWindowDecoder(c.Window),
		}

	// Note: Iterables in CoGBK are handled in datasource.go instead.
	case coder.Iterable:
		return &iterableDecoder{
			t:   c.T.Type(),
			dec: MakeElementDecoder(c.Components[0]),
		}

	case coder.WindowedValue:
		return &windowedValueDecoder{
			elm: MakeElementDecoder(c.Components[0]),
			win: MakeWindowDecoder(c.Window),
		}

	case coder.ParamWindowedValue:
		return &paramWindowedValueDecoder{
			elm: MakeElementDecoder(c.Components[0]),
			win: MakeWindowDecoder(c.Window),
		}

	case coder.Timer:
		return &timerDecoder{
			elm: MakeElementDecoder(c.Components[0]),
		}

	case coder.Row:
		dec, err := coder.RowDecoderForStruct(c.T.Type())
		if err != nil {
			panic(err)
		}
		return &rowDecoder{
			dec: dec,
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
	return coder.EncodeBytes(data, w)
}

type bytesDecoder struct{}

func (*bytesDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: size (varint) + raw data
	data, err := coder.DecodeBytes(r)
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

type lpEncoder struct {
	enc ElementEncoder
	buf bytes.Buffer
}

func (c *lpEncoder) Encode(val *FullValue, w io.Writer) error {
	// (1) Call encode
	if err := c.enc.Encode(val, &c.buf); err != nil {
		return err
	}

	// (2) Add length prefix
	data := c.buf.Bytes()
	size := len(data)
	if err := coder.EncodeVarInt((int64)(size), w); err != nil {
		return err
	}
	_, err := w.Write(data)
	c.buf.Reset()
	return err
}

type lpDecoder struct {
	dec ElementDecoder
	r   io.LimitedReader
}

func (c *lpDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// (1) Read length-prefixed encoded data
	size, err := coder.DecodeVarInt(r)
	if err != nil {
		return err
	}
	c.r = io.LimitedReader{R: r, N: size}

	// (2) Call decode
	return c.dec.DecodeTo(&c.r, fv)
}

func (c *lpDecoder) Decode(r io.Reader) (*FullValue, error) {
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

type iterableEncoder struct {
	t   reflect.Type
	enc ElementEncoder
}

func (c *iterableEncoder) Encode(val *FullValue, w io.Writer) error {
	// Do a reflect, get the length.
	rv := reflect.ValueOf(val.Elm)
	size := rv.Len()
	if err := coder.EncodeInt32((int32)(size), w); err != nil {
		return err
	}
	var e FullValue
	for i := 0; i < size; i++ {
		e.Elm = rv.Index(i).Interface()
		err := c.enc.Encode(&e, w)
		if err != nil {
			return err
		}
	}
	return nil
}

type iterableDecoder struct {
	t   reflect.Type
	dec ElementDecoder
}

func (c *iterableDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// (1) Read count prefixed encoded data

	size, err := coder.DecodeInt32(r)
	if err != nil {
		return err
	}
	n := int(size)
	switch {
	case n >= 0:
		rv, err := c.decodeToSlice(int(n), r)
		if err != nil {
			return err
		}
		*fv = FullValue{Elm: rv.Interface()}
		return nil
	case n == -1:
		rv := reflect.MakeSlice(c.t, 0, 0)
		chunk, err := coder.DecodeVarInt(r)
		if err != nil {
			return err
		}
		for chunk != 0 {
			rvi, err := c.decodeToSlice(int(chunk), r)
			if err != nil {
				return err
			}
			rv = reflect.AppendSlice(rv, rvi)
			chunk, err = coder.DecodeVarInt(r)
			if err != nil {
				return err
			}
		}
		*fv = FullValue{Elm: rv.Interface()}
	}

	return nil
}

func (c *iterableDecoder) decodeToSlice(n int, r io.Reader) (reflect.Value, error) {
	var e FullValue
	rv := reflect.MakeSlice(c.t, n, n)
	for i := 0; i < int(n); i++ {
		err := c.dec.DecodeTo(r, &e)
		if err != nil {
			return reflect.Value{}, err
		}
		if e.Elm != nil {
			rv.Index(i).Set(reflect.ValueOf(e.Elm))
		} else {
			rv.Index(i).Set(reflect.ValueOf(e.Windows[0]))
		}
	}
	return rv, nil
}

func (c *iterableDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := c.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type arrayDecoder struct {
	t   reflect.Type // array type
	dec ElementDecoder
}

func (c *arrayDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// (1) Read count prefixed encoded data

	size, err := coder.DecodeInt32(r)
	if err != nil {
		return err
	}
	n := int(size)
	if n != c.t.Len() {
		return errors.Errorf("array len mismatch. decoding %v but only have %v elements.", c.t, n)
	}
	switch {
	case n >= 0:
		rv := reflect.New(c.t).Elem()
		var e FullValue
		for i := 0; i < int(n); i++ {
			err := c.dec.DecodeTo(r, &e)
			if err != nil {
				return err
			}
			if e.Elm != nil {
				rv.Index(i).Set(reflect.ValueOf(e.Elm))
			}
		}
		*fv = FullValue{Elm: rv.Interface()}
		return nil
	default:
		return errors.Errorf("unable to decode array with iterable marker %v", n)
	}
}

func (c *arrayDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := c.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type windowedValueEncoder struct {
	elm ElementEncoder
	win WindowEncoder
}

func (e *windowedValueEncoder) Encode(val *FullValue, w io.Writer) error {
	if err := EncodeWindowedValueHeader(e.win, val.Windows, val.Timestamp, w); err != nil {
		return err
	}
	return e.elm.Encode(val, w)
}

type windowedValueDecoder struct {
	elm ElementDecoder
	win WindowDecoder
}

func (d *windowedValueDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: beam utf8 string (length prefix + run of bytes)
	w, et, err := DecodeWindowedValueHeader(d.win, r)
	if err != nil {
		return err
	}
	if err := d.elm.DecodeTo(r, fv); err != nil {
		return err
	}
	fv.Windows = w
	fv.Timestamp = et
	return nil
}

func (d *windowedValueDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type paramWindowedValueEncoder struct {
	elm ElementEncoder
	win WindowEncoder
}

func (*paramWindowedValueEncoder) Encode(val *FullValue, w io.Writer) error {
	// Encoding: beam utf8 string (length prefix + run of bytes)
	return coder.EncodeStringUTF8(val.Elm.(string), w)
}

type paramWindowedValueDecoder struct {
	elm ElementDecoder
	win WindowDecoder
}

func (*paramWindowedValueDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	// Encoding: beam utf8 string (length prefix + run of bytes)
	f, err := coder.DecodeStringUTF8(r)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: f}
	return nil
}

func (d *paramWindowedValueDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type timerEncoder struct {
	elm ElementEncoder
}

func (e *timerEncoder) Encode(val *FullValue, w io.Writer) error {
	return e.elm.Encode(val, w)
}

type timerDecoder struct {
	elm ElementDecoder
}

func (d *timerDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	return d.elm.DecodeTo(r, fv)
}

func (d *timerDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type rowEncoder struct {
	enc func(interface{}, io.Writer) error
}

func (e *rowEncoder) Encode(val *FullValue, w io.Writer) error {
	return e.enc(val.Elm, w)
}

type rowDecoder struct {
	dec func(r io.Reader) (interface{}, error)
}

func (d *rowDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	v, err := d.dec(r)
	if err != nil {
		return err
	}
	*fv = FullValue{Elm: v}
	return nil
}

func (d *rowDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
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
	// DecodeSingle decodes a single window from the given reader.
	DecodeSingle(io.Reader) (typex.Window, error)
}

// MakeWindowEncoder returns a WindowEncoder for the given window coder.
func MakeWindowEncoder(c *coder.WindowCoder) WindowEncoder {
	if c.Payload != "" {
		return &payloadWindowEncoder{payload: []byte(c.Payload)}
	}
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
	var w WindowDecoder
	switch c.Kind {
	case coder.GlobalWindow:
		w = &globalWindowDecoder{}

	case coder.IntervalWindow:
		w = &intervalWindowDecoder{}

	default:
		panic(fmt.Sprintf("Unexpected window coder: %v", c))
	}
	if c.Payload != "" {
		w = &payloadWindowDecoder{dec: w, payload: c.Payload, payloadR: strings.NewReader(c.Payload)}
	}
	return w
}

// wrappedWindowEncoder wraps a WindowEncoder for the ElementEncoder interface.
type wrappedWindowEncoder struct {
	enc WindowEncoder
}

func (e *wrappedWindowEncoder) Encode(val *FullValue, w io.Writer) error {
	if len(val.Windows) == 0 {
		return nil
	}
	return e.enc.EncodeSingle(val.Windows[0], w)
}

// wrappedWindowDecoder wraps a WindowDecoder for the ElementDecoder interface.
type wrappedWindowDecoder struct {
	dec WindowDecoder
}

func (d *wrappedWindowDecoder) DecodeTo(r io.Reader, fv *FullValue) error {
	ws, err := d.dec.DecodeSingle(r)
	if err != nil {
		return err
	}
	fv.Windows = []typex.Window{ws}
	return nil
}

func (d *wrappedWindowDecoder) Decode(r io.Reader) (*FullValue, error) {
	fv := &FullValue{}
	if err := d.DecodeTo(r, fv); err != nil {
		return nil, err
	}
	return fv, nil
}

type payloadWindowEncoder struct {
	payload []byte
}

func (e *payloadWindowEncoder) Encode(ws []typex.Window, w io.Writer) error {
	_, err := w.Write(e.payload)
	return err
}

func (e *payloadWindowEncoder) EncodeSingle(ws typex.Window, w io.Writer) error {
	_, err := w.Write(e.payload)
	return err
}

type payloadWindowDecoder struct {
	dec      WindowDecoder
	payload  string
	payloadR *strings.Reader
}

func (d *payloadWindowDecoder) Decode(r io.Reader) ([]typex.Window, error) {
	d.payloadR.Reset(d.payload)
	return d.dec.Decode(d.payloadR)
}

func (d *payloadWindowDecoder) DecodeSingle(r io.Reader) (typex.Window, error) {
	d.payloadR.Reset(d.payload)
	return d.dec.DecodeSingle(d.payloadR)
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
func (*globalWindowDecoder) DecodeSingle(r io.Reader) (typex.Window, error) {
	return window.SingleGlobalWindow[0], nil
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

func (d *intervalWindowDecoder) Decode(r io.Reader) ([]typex.Window, error) {
	// Encoding: upper bound and duration

	n, err := coder.DecodeInt32(r) // #windows

	ret := make([]typex.Window, n, n)
	for i := int32(0); i < n; i++ {
		w, err := d.DecodeSingle(r)
		if err != nil {
			return nil, err
		}
		ret[i] = w
	}
	return ret, err
}

func (*intervalWindowDecoder) DecodeSingle(r io.Reader) (typex.Window, error) {
	end, err := coder.DecodeEventTime(r)
	if err != nil {
		return nil, err
	}
	duration, err := coder.DecodeVarInt(r)
	if err != nil {
		return nil, err
	}
	return window.IntervalWindow{Start: mtime.FromMilliseconds(end.Milliseconds() - int64(duration)), End: end}, nil
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
