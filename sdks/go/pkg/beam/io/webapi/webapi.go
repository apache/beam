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

package webapi

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"reflect"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*wrappedCallerOnlyUserType)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Request)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*Response)(nil)).Elem())
	register.Emitter1[*Response]()
	register.DoFn3x1[context.Context, *Request, func(*Response), error](&callerFn{})
}

// Caller is an interface to a Web API endpoint.
type Caller interface {

	// Call a Web API endpoint with a Request, yielding a Response.
	Call(ctx context.Context, request *Request) (*Response, error)
}

// SetupTeardown interfaces methods called during a setup and teardown DoFn lifecycle.
// Some clients to Web APIs may need to establish resources or network connections prior to
// calling a Web API. This is a separate interface from a Caller since some Web API endpoints
// do not require this. An internal hybrid interface is hidden for user convenience so that
// a developer need only provide what makes sense for the API read or write task.
type SetupTeardown interface {

	// Setup is called during a DoFn setup lifecycle method.
	// Clients that need to instantiate resources would be performed within an implementation of this method.
	Setup(ctx context.Context) error

	// Teardown is called during a DoFn teardown lifecycle method.
	// Clients that need to destroy or close resources would be performed within an implementation of this method.
	Teardown(ctx context.Context) error
}

// Request holds the encoded payload of a Web API request.
// Caller implementations are responsible for decoding the Payload into the needed type required
// to fulfill the Web API request.
type Request struct {

	// Payload is the encoded Web API request.
	Payload []byte `beam:"payload"`
}

// Response holds the encoded payload of a Web API response.
// Caller implementations are responsible for encoding the Payload into the needed type required
// to represent the Web API response.
type Response struct {

	// Payload is the encoded Web API response.
	Payload []byte `beam:"payload"`
}

// newCallerFn instantiates a callerFn from a Caller. It wraps an encoded representation of the user defined Caller in a
// wrappedCallerOnlyUserType that serves as a noop SetupTeardown. This allows the user developer
// to only concern themselves with the smallest interface necessary to achieve their Web API read or write task
// while also maintaining a single code path for encoding and decoding callerFn's dependencies.
func newCallerFn(caller Caller) (*callerFn, error) {
	t := reflect.TypeOf(caller)
	enc := beam.NewElementEncoder(t)
	buf := bytes.Buffer{}
	if err := enc.Encode(caller, &buf); err != nil {
		return nil, fmt.Errorf("Encode(%T) err %w", caller, err)
	}
	wrapped := &wrappedCallerOnlyUserType{
		SerializedIFace: buf.Bytes(),
		WrappedIFace: beam.EncodedType{
			T: t,
		},
	}
	return newCallerFnFromHybridIFace(wrapped)
}

// newCallerFnFromHybridIFace instantiates a callerFn from a hybrid Caller and SetupTeardown interface.
// It encodes the type using a beam.ElementEncoder, storing these artifacts in the callerFn instance.
func newCallerFnFromHybridIFace(caller callerSetupTeardown) (*callerFn, error) {
	t := reflect.TypeOf(caller)
	enc := beam.NewElementEncoder(t)
	buf := bytes.Buffer{}
	if err := enc.Encode(caller, &buf); err != nil {
		return nil, fmt.Errorf("Encode(%T) err %w", caller, err)
	}
	return &callerFn{
		SerializedIFace: buf.Bytes(),
		WrappedIFace: beam.EncodedType{
			T: t,
		},
	}, nil
}

// callerFn is a DoFn processes Request elements into Response elements by
// invoking a user defined Caller or callerSetupTeardown.
type callerFn struct {
	SerializedIFace []byte           `json:"serialized_i_face"`
	WrappedIFace    beam.EncodedType `json:"wrapped_i_face"`
	iface           callerSetupTeardown
}

// Setup decodes the user defined Caller or callerSetupTeardown.
// It forwards the ctx to successfully decoded SetupTeardown's Setup method.
func (fn *callerFn) Setup(ctx context.Context) error {
	dec := beam.NewElementDecoder(fn.WrappedIFace.T)
	buf := bytes.NewReader(fn.SerializedIFace)
	iface, err := dec.Decode(buf)
	if err != nil {
		return fmt.Errorf("dec.Decode(%T) err: %w", fn.WrappedIFace.T, err)
	}

	cst, ok := iface.(callerSetupTeardown)
	if !ok {
		return fmt.Errorf("%T is not a callerSetupTeardown type", iface)
	}

	fn.iface = cst

	return fn.iface.Setup(ctx)
}

// ProcessElement invokes a Caller's Call method with the Request and emitting the Response upon success.
func (fn *callerFn) ProcessElement(ctx context.Context, req *Request, emit func(*Response)) error {
	if fn.iface == nil {
		return fmt.Errorf("callerFn iface is nil")
	}
	resp, err := fn.iface.Call(ctx, req)
	if err != nil {
		return fmt.Errorf("error Call(%T) %w", fn.iface, err)
	}
	emit(resp)
	return nil
}

// Teardown forwards the ctx to the decoded SetupTeardown's Teardown method.
func (fn *callerFn) Teardown(ctx context.Context) error {
	if fn.iface != nil {
		return fn.iface.Teardown(ctx)
	}
	return nil
}

// callerSetupTeardown is a hybrid Caller and SetupTeardown interface.
// It is hidden to allow the user developer to focus on Web API needs for situations that just need
// a Caller implementation or those that need both.
type callerSetupTeardown interface {
	Caller
	SetupTeardown
}

// wrappedCallerOnlyUserType is a callerSetupTeardown hybrid interface useful for settings where a user developer
// need only concern with using a Caller for their Web API read and write tasks. The design goals of this type
// are to enable a single code path for the callerFn when encoding and decoding its dependencies.
type wrappedCallerOnlyUserType struct {
	SerializedIFace []byte           `json:"serialized_i_face"`
	WrappedIFace    beam.EncodedType `json:"wrapped_i_face"`
	iface           Caller
}

// Call forwards the ctx and request to a user developer's Caller Call method.
func (w *wrappedCallerOnlyUserType) Call(ctx context.Context, request *Request) (*Response, error) {
	if w.iface == nil {
		return nil, fmt.Errorf("%T iface is nil", w)
	}
	return w.iface.Call(ctx, request)
}

// Setup decodes a user developer's Caller.
func (w *wrappedCallerOnlyUserType) Setup(_ context.Context) error {
	dec := beam.NewElementDecoder(w.WrappedIFace.T)
	buf := bytes.NewReader(w.SerializedIFace)
	iface, err := dec.Decode(buf)
	if err != nil {
		return fmt.Errorf("dec.Decode(%T) err: %w", w.WrappedIFace.T, err)
	}

	cst, ok := iface.(Caller)
	if !ok {
		return fmt.Errorf("%T is not a Caller type", iface)
	}

	w.iface = cst

	return nil
}

// Teardown is a noop method.
func (w *wrappedCallerOnlyUserType) Teardown(_ context.Context) error {
	return nil
}
