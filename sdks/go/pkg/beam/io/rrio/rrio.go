package rrio

import (
	"bytes"
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"reflect"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*configuration)(nil)))
	beam.RegisterDoFn(reflect.TypeOf((*callerFn)(nil)))
}

type Option interface {
	apply(config *configuration)
}

func WithSetupTeardown(impl SetupTeardown) Option {
	return &withSetupTeardown{
		impl: impl,
	}
}

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

type Request struct {
	Payload []byte
}

type Response struct {
	Payload []byte
}

type ApiIOError struct {
	Message string `beam:"message"`
}

type Caller interface {
	Call(ctx context.Context, request *Request) (*Response, error)
}

type SetupTeardown interface {
	Setup(ctx context.Context) error
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
			Message: err.Error(),
		})
		return nil
	}
	output(resp)
	return nil
}

type noOpSetupTeardown struct{}

func (client *noOpSetupTeardown) Setup(ctx context.Context) error {
	// No Op
	return nil
}

func (client *noOpSetupTeardown) Teardown(ctx context.Context) error {
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
