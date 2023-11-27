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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// TODO(herohde) 1/29/2018: using FullValue for nested KVs is somewhat of a hack
// and is errorprone. Consider making it a more explicit data structure.

// FullValue represents the full runtime value for a data element, incl. the
// implicit context. The result of a GBK or CoGBK is not a single FullValue.
// The consumer is responsible for converting the values to the correct type.
// To represent a nested KV with FullValues, assign a *FullValue to Elm/Elm2.
type FullValue struct {
	Elm  any // Element or KV key.
	Elm2 any // KV value, if not invalid

	Timestamp    typex.EventTime
	Windows      []typex.Window
	Pane         typex.PaneInfo
	Continuation sdf.ProcessContinuation
}

func (v *FullValue) String() string {
	if v.Elm2 == nil {
		return fmt.Sprintf("%v [@%v:%v:%v]", v.Elm, v.Timestamp, v.Windows, v.Pane)
	}
	return fmt.Sprintf("KV<%v,%v> [@%v:%v:%v]", v.Elm, v.Elm2, v.Timestamp, v.Windows, v.Pane)
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

// Open returns the Stream from the start of the in-memory ReStream.
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
func Convert(v any, to reflect.Type) any {
	from := reflect.TypeOf(v)
	return ConvertFn(from, to)(v)
}

// ConvertFn returns a function that converts type of the runtime value to the desired one. It is needed
// to drop the universal type and convert Aggregate types.
func ConvertFn(from, to reflect.Type) func(any) any {
	switch {
	case from == to:
		return identity

	case typex.IsUniversal(from):
		return universal

	case typex.IsList(from) && typex.IsList(to):
		fromE := from.Elem()
		toE := to.Elem()
		cvtFn := ConvertFn(fromE, toE)
		return func(v any) any {
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
		return func(v any) any {
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
func identity(v any) any {
	return v
}

// universal drops the universal type and re-interfaces it to the actual one.
func universal(v any) any {
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

// singleUseReStream is a decode on demand ReStream.
// Can only produce a single Stream because it consumes the reader.
// Must not be used for streams that might be re-iterated, causing Open
// to be called twice.
type singleUseReStream struct {
	r    io.Reader
	d    ElementDecoder
	size int // The number of elements in this stream.
}

// Open returns the Stream from the start of the in-memory reader. Returns error if called twice.
func (n *singleUseReStream) Open() (Stream, error) {
	if n.r == nil {
		return nil, errors.New("decodeReStream opened twice")
	}
	ret := &decodeStream{r: n.r, d: n.d, size: n.size}
	n.r = nil
	n.d = nil
	return ret, nil
}

// decodeStream is a decode on demand Stream, that decodes size elements from the provided
// io.Reader.
type decodeStream struct {
	r          io.Reader
	d          ElementDecoder
	next, size int
	ret        FullValue
}

// Close causes subsequent calls to Read to return io.EOF, and drains the remaining element count
// from the reader.
func (s *decodeStream) Close() error {
	// On close, if next != size, we must iterate through the rest of the decoding
	// until the reader is drained. Otherwise we corrupt the read for the next element.
	//
	// TODO(https://github.com/apache/beam/issues/22901):
	// Optimize the case where we have length prefixed values
	// so we can avoid allocating the values in the first place.
	for s.next < s.size {
		err := s.d.DecodeTo(s.r, &s.ret)
		if err != nil {
			return errors.Wrap(err, "decodeStream value decode failed on close")
		}
		s.next++
	}
	s.r = nil
	s.d = nil
	s.ret = FullValue{}
	return nil
}

// Read produces the next value in the stream.
func (s *decodeStream) Read() (*FullValue, error) {
	if s.r == nil || s.next == s.size {
		return nil, io.EOF
	}
	err := s.d.DecodeTo(s.r, &s.ret)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, errors.Wrap(err, "decodeStream value decode failed")
	}
	s.next++
	return &s.ret, nil
}

// singleUseMultiChunkReStream is a decode on demand restream, that can handle a multi-chunk streams.
// Can only produce a single Stream because it consumes the reader.
// Must not be used for streams that might be re-iterated, causing Open to be called twice.
type singleUseMultiChunkReStream struct {
	r *byteCountReader
	d ElementDecoder

	open func(*byteCountReader) (Stream, error)
}

// Open returns the Stream from the start of the in-memory ReStream. Returns error if called twice.
func (n *singleUseMultiChunkReStream) Open() (Stream, error) {
	if n.r == nil {
		return nil, errors.New("decodeReStream opened twice")
	}
	ret := &decodeMultiChunkStream{r: n.r, d: n.d, open: n.open}
	n.r = nil
	n.d = nil
	return ret, nil
}

// decodeMultiChunkStream is a simple decode on demand Stream.
type decodeMultiChunkStream struct {
	r           *byteCountReader
	d           ElementDecoder
	ret         FullValue
	next, chunk int64

	open   func(r *byteCountReader) (Stream, error)
	stream Stream
}

// Close releases the buffer, closing the stream.
func (s *decodeMultiChunkStream) Close() error {
	// On close, if next < size, we must iterate through the rest of the decoding
	// until the reader is drained. Otherwise we corrupt the read for the next element.
	// TODO(https://github.com/apache/beam/issues/22901):
	// Optimize the case where we have length prefixed values
	// so we can avoid allocating the values in the first place.

	for {
		// If we have a stream, we're finished with the available bytes from the reader,
		// so we move to close it after this loop.
		if s.stream != nil {
			break
		}
		// Drain the whole available iterable to ensure the reader is in the right position.
		_, err := s.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	if s.stream != nil {
		s.stream.Close()
		s.stream = nil
	}
	s.r = nil
	s.d = nil
	s.ret = FullValue{}
	return nil
}

// Read produces the next value in the stream.
func (s *decodeMultiChunkStream) Read() (*FullValue, error) {
	// No Reader? We're done.
	if s.r == nil {
		return nil, io.EOF
	}
	// If we have a stream already, use that.
	if s.stream != nil {
		return s.stream.Read()
	}

	// If our next value is at the chunk size, then re-set the chunk and size.
	if s.next == s.chunk {
		s.chunk = 0
		s.next = 0
	}

	// We're at the start of a chunk, see if there's a next chunk.
	if s.chunk == 0 && s.next == 0 {
		chunk, err := coder.DecodeVarInt(s.r.reader)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, errors.Wrap(err, "decodeMultiChunkStream chunk size decoding failed")
		}
		s.chunk = chunk
	}
	switch {
	case s.chunk == 0:
		// If the chunk is still 0, then we're done.
		s.r = nil
		s.d = nil
		s.ret = FullValue{}
		return nil, io.EOF
	case s.chunk > 0:
		err := s.d.DecodeTo(s.r, &s.ret)
		if err != nil {
			return nil, errors.Wrap(err, "decodeStream value decode failed")
		}
		s.next++
		return &s.ret, nil
	case s.chunk == -1:
		// State Backed Iterable!
		stream, err := s.open(s.r)
		if err != nil {
			return nil, err
		}
		s.stream = stream
		return s.stream.Read()
	}
	return nil, errors.Errorf("decodeMultiChunkStream invalid chunk size: %v", s.chunk)
}
