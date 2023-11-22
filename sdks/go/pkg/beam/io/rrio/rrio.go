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

// Package rrio supports reading from and writing to Web APIs.
package rrio

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

var ()

func init() {
	beam.RegisterType(reflect.TypeOf((*configuration)(nil)))
	beam.RegisterType(reflect.TypeOf((*noOpSetupTeardown)(nil)))
	beam.RegisterDoFn(reflect.TypeOf((*callerFn)(nil)))
}

var ()

// Option applies optional features to Execute.
type Option interface {
	apply(config *configuration)
}

// WithSetupTeardown applies an optional SetupTeardown that Execute invokes
// during its DoFn's setup and teardown methods.
func WithSetupTeardown(impl SetupTeardown) Option {
	return &withSetupTeardown{
		impl: impl,
	}
}

// Execute a Caller with requests.
//
// Returns output and failure PCollections depending on the outcome of invoking
// the Caller's call func. Additional opts apply additional Option features
// to the transform.
func Execute(s beam.Scope, caller Caller, requests beam.PCollection, opts ...Option) (beam.PCollection, beam.PCollection) {
	config := &configuration{
		caller:        caller,
		setupTeardown: &noOpSetupTeardown{},
	}
	for _, opt := range opts {
		opt.apply(config)
	}
	output, failures, err := tryExecute(s.Scope("rrio.Execute"), config, requests)
	if err != nil {
		panic(err)
	}
	return output, failures
}

func tryExecute(s beam.Scope, config *configuration, requests beam.PCollection) (beam.PCollection, beam.PCollection, error) {
	var output, failures beam.PCollection

	if err := config.encode(); err != nil {
		return output, failures, err
	}

	output, failures = beam.ParDo2(s.Scope("rrio.callerFn"), &callerFn{
		Config: config,
	}, requests)

	return output, failures, nil
}

// Request is a request represented as a byte array payload.
type Request struct {
	Payload []byte
}

// Response is a response represented as a byte array payload.
type Response struct {
	Payload []byte
}

// ApiIOError encapsulates an error and its associated Request.
type ApiIOError struct {
	Request      *Request  `beam:"request"`
	Message      string    `beam:"message"`
	ObservedTime time.Time `beam:"observed_time"`
}

// Caller invokes an API with a Request, yielding either a Response or error.
//
// User code is responsible to convert from a Request's Payload and to
// Response's Payload after successful invocation of an API call. It is
// not recommended to instantiate the API client for each Call invocation.
// Consider implementing the SetupTeardown interface and providing this via
// the WithSetupTeardown Option. Note that a Caller and SetupTeardown can
// represent the same struct type.
type Caller interface {

	// Call an API with a Request.
	Call(ctx context.Context, request *Request) (*Response, error)
}

// SetupTeardown performs the setup and teardown of an API client. Execute's
// DoFn invokes SetupTeardown during its own setup and teardown methods, if
// provided when calling Execute with the WithSetupTeardown Option.
type SetupTeardown interface {
	// Setup an API client.
	Setup(ctx context.Context) error

	// Teardown an API client.
	Teardown(ctx context.Context) error
}

type callerFn struct {
	Config *configuration `json:"configuration"`
}

func (fn *callerFn) Setup(ctx context.Context) error {
	if err := fn.Config.decode(); err != nil {
		return err
	}
	return fn.Config.setupTeardown.Setup(ctx)
}

func (fn *callerFn) Teardown(ctx context.Context) error {
	return fn.Config.setupTeardown.Teardown(ctx)
}

func (fn *callerFn) ProcessElement(ctx context.Context, request *Request, output func(*Response), failures func(*ApiIOError)) error {
	if fn.Config.caller == nil {
		return fmt.Errorf("%s not instantiated", fn.Config.WrappedCaller.T.String())
	}
	resp, err := fn.Config.caller.Call(ctx, request)
	if err != nil {
		failures(&ApiIOError{
			Request:      request,
			Message:      err.Error(),
			ObservedTime: time.Now(),
		})
		return nil
	}
	output(resp)
	return nil
}

type noOpSetupTeardown struct{}

func (*noOpSetupTeardown) Setup(ctx context.Context) error {
	// No Op
	return nil
}

func (*noOpSetupTeardown) Teardown(ctx context.Context) error {
	// No Op
	return nil
}

type configuration struct {
	SerializedCaller        []byte           `json:"serialized_caller"`
	SerializedSetupTeardown []byte           `json:"serialized_setup_teardown"`
	WrappedCaller           beam.EncodedType `json:"caller"`
	WrappedSetupTeardown    beam.EncodedType `json:"setup_teardown"`
	caller                  Caller
	setupTeardown           SetupTeardown
}

func (config *configuration) encode() error {
	if err := config.encodeCaller(); err != nil {
		return fmt.Errorf("error encoding %T, err %w", config.caller, err)
	}

	if err := config.encodeSetupTeardown(); err != nil {
		return fmt.Errorf("error encoding %T, err %w", config.setupTeardown, err)
	}

	if _, ok := config.caller.(SetupTeardown); ok {
		config.setupTeardown = config.caller.(SetupTeardown)
	}

	return nil
}

func (config *configuration) encodeCaller() error {
	t := reflect.TypeOf(config.caller)

	buf := bytes.Buffer{}
	enc := beam.NewElementEncoder(t)
	if err := enc.Encode(config.caller, &buf); err != nil {
		return err
	}
	config.SerializedCaller = buf.Bytes()
	config.WrappedCaller = beam.EncodedType{
		T: t,
	}

	return nil
}

func (config *configuration) encodeSetupTeardown() error {
	t := reflect.TypeOf(config.setupTeardown)

	buf := bytes.Buffer{}
	enc := beam.NewElementEncoder(t)
	if err := enc.Encode(config.setupTeardown, &buf); err != nil {
		return err
	}
	config.SerializedSetupTeardown = buf.Bytes()
	config.WrappedSetupTeardown = beam.EncodedType{
		T: t,
	}

	return nil
}

func (config *configuration) decode() error {
	if err := config.decodeCaller(); err != nil {
		return fmt.Errorf("error decoding %T, err %w", config.caller, err)
	}

	if err := config.decodeSetupTeardown(); err != nil {
		return fmt.Errorf("error decoding %T, err %w", config.setupTeardown, err)
	}

	if _, ok := config.caller.(SetupTeardown); ok {
		config.setupTeardown = config.caller.(SetupTeardown)
	}

	return nil
}

func (config *configuration) decodeCaller() error {
	dec := beam.NewElementDecoder(config.WrappedCaller.T)
	buf := bytes.NewReader(config.SerializedCaller)
	iface, err := dec.Decode(buf)
	if err != nil {
		return err
	}
	config.caller = iface.(Caller)
	return nil
}

func (config *configuration) decodeSetupTeardown() error {
	dec := beam.NewElementDecoder(config.WrappedSetupTeardown.T)
	buf := bytes.NewReader(config.SerializedSetupTeardown)
	iface, err := dec.Decode(buf)
	if err != nil {
		return err
	}
	config.setupTeardown = iface.(SetupTeardown)

	return nil
}

type withSetupTeardown struct {
	impl SetupTeardown
}

func (opt *withSetupTeardown) apply(config *configuration) {
	config.setupTeardown = opt.impl
}
