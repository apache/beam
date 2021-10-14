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

// Package hooks allows runners to tailor execution of the worker harness.
//
// Examples of customization:
//
// gRPC integration
// session recording
// profile recording
//
// Registration methods for hooks must be called prior to calling beam.Init()
// Request methods for hooks must be called as part of building the pipeline
// request for the runner's Execute method.
package hooks

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

var defaultRegistry = newRegistry(runtime.GlobalOptions)

type registry struct {
	mu               sync.Mutex
	hookRegistry     map[string]HookFactory
	enabledHooks     map[string][]string
	enabledHookOrder []string
	activeHooks      map[string]Hook

	options *runtime.Options
}

func newRegistry(options *runtime.Options) *registry {
	return &registry{
		hookRegistry: make(map[string]HookFactory),
		enabledHooks: make(map[string][]string),
		activeHooks:  make(map[string]Hook),
		options:      options,
	}
}

// A Hook is a set of hooks to run at various stages of executing a
// pipeline.
type Hook struct {
	// Init is called once at the startup of the worker prior to
	// connecting to the FnAPI services.
	Init InitHook
	// Req is called each time the worker handles a FnAPI instruction request.
	Req RequestHook
	// Resp is called each time the worker generates a FnAPI instruction response.
	Resp ResponseHook
}

// InitHook is a hook that is called when the harness
// initializes.
type InitHook func(context.Context) (context.Context, error)

// HookFactory is a function that produces a Hook from the supplied arguments.
type HookFactory func([]string) Hook

func (r *registry) RegisterHook(name string, h HookFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hookRegistry[name] = h
}

// RegisterHook registers a Hook for the
// supplied identifier.
func RegisterHook(name string, h HookFactory) {
	defaultRegistry.RegisterHook(name, h)
}

func (r *registry) RunInitHooks(ctx context.Context) (context.Context, error) {
	// If an init hook fails to complete, the invariants of the
	// system are compromised and we can't run a workflow.
	// The hooks can run in any order. They should not be
	// interdependent or interfere with each other.
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.enabledHookOrder {
		h := r.activeHooks[n]
		if h.Init != nil {
			var err error
			if ctx, err = h.Init(ctx); err != nil {
				return ctx, err
			}
		}
	}
	return ctx, nil
}

// RunInitHooks runs the init hooks.
func RunInitHooks(ctx context.Context) (context.Context, error) {
	return defaultRegistry.RunInitHooks(ctx)
}

// RequestHook is called when handling a FnAPI instruction. It can return an updated
// context to pass additional information to downstream callers, or return the
// original context provided.
type RequestHook func(context.Context, *fnpb.InstructionRequest) (context.Context, error)

func (r *registry) RunRequestHooks(ctx context.Context, req *fnpb.InstructionRequest) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	// The request hooks should not modify the request.
	for _, n := range r.enabledHookOrder {
		h := r.activeHooks[n]
		if h.Req != nil {
			var err error
			if ctx, err = h.Req(ctx, req); err != nil {
				log.Infof(ctx, "request hook %s failed: %v", n, err)
			}
		}
	}
	return ctx
}

// RunRequestHooks runs the hooks that handle a FnAPI request.
func RunRequestHooks(ctx context.Context, req *fnpb.InstructionRequest) context.Context {
	return defaultRegistry.RunRequestHooks(ctx, req)
}

// ResponseHook is called when sending a FnAPI instruction response.
type ResponseHook func(context.Context, *fnpb.InstructionRequest, *fnpb.InstructionResponse) error

func (r *registry) RunResponseHooks(ctx context.Context, req *fnpb.InstructionRequest, resp *fnpb.InstructionResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.enabledHookOrder {
		h := r.activeHooks[n]
		if h.Resp != nil {
			if err := h.Resp(ctx, req, resp); err != nil {
				log.Infof(ctx, "response hook %s failed: %v", n, err)
			}
		}
	}
}

// RunResponseHooks runs the hooks that handle a FnAPI response.
func RunResponseHooks(ctx context.Context, req *fnpb.InstructionRequest, resp *fnpb.InstructionResponse) {
	defaultRegistry.RunResponseHooks(ctx, req, resp)
}

const (
	orderKey = "hookOrder"
	hookKey  = "hooks"
)

func (r *registry) SerializeHooksToOptions() {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, err := json.Marshal(r.enabledHooks)
	if err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrap(err, "Couldn't serialize hooks"))
	}
	r.options.Set(hookKey, string(data))

	orderData, err := json.Marshal(r.enabledHookOrder)
	if err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrap(err, "Couldn't serialize hooks"))
	}
	r.options.Set(orderKey, string(orderData))
}

// SerializeHooksToOptions serializes the activated hooks and their configuration into a JSON string
// that can be deserialized later by the runner.
func SerializeHooksToOptions() {
	defaultRegistry.SerializeHooksToOptions()
}

func (r *registry) DeserializeHooksFromOptions(ctx context.Context) {
	hookOrder := r.options.Get(orderKey)
	if hookOrder == "" {
		log.Warn(ctx, "SerializeHooksToOptions was never called. No hooks enabled")
		return
	}
	cfg := r.options.Get(hookKey)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := json.Unmarshal([]byte(hookOrder), &r.enabledHookOrder); err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrapf(err, "DeserializeHooks failed on input %q", hookOrder))
	}
	if err := json.Unmarshal([]byte(cfg), &r.enabledHooks); err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrapf(err, "DeserializeHooks failed on input %q", cfg))
	}

	for _, h := range r.enabledHookOrder {
		opts := r.enabledHooks[h]
		r.activeHooks[h] = r.hookRegistry[h](opts)
	}
}

// DeserializeHooksFromOptions extracts the hook configuration information from the options and configures
// the hooks with the supplied options.
func DeserializeHooksFromOptions(ctx context.Context) {
	defaultRegistry.DeserializeHooksFromOptions(ctx)
}

func (r *registry) EnableHook(name string, args ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.hookRegistry[name]; !ok {
		return errors.Errorf("EnableHook: hook %s not found", name)
	}
	r.disableHook(name)
	r.enabledHookOrder = append(r.enabledHookOrder, name)
	r.enabledHooks[name] = args
	return nil
}

// EnableHook enables the hook to be run for the pipeline. It will be
// receive the supplied args when the pipeline executes.
// Repeat calls for the same hook will overwrite previous args,
// and move the hook to the end of the ordering.
// Multiple options can be provided,
// as this is necessary if a hook wants to compose behavior.
func EnableHook(name string, args ...string) error {
	return defaultRegistry.EnableHook(name, args...)
}

func (r *registry) IsEnabled(name string) (bool, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	opts, ok := r.enabledHooks[name]
	return ok, opts
}

// IsEnabled returns true and the registered options if the hook is
// already enabled.
func IsEnabled(name string) (bool, []string) {
	return defaultRegistry.IsEnabled(name)
}

// disableHook must be called in while holding mu.
func (r *registry) disableHook(name string) {
	for i, h := range r.enabledHookOrder {
		if h == name {
			r.enabledHookOrder = append(r.enabledHookOrder[:i], r.enabledHookOrder[i+1:]...)
		}
	}
	delete(r.enabledHooks, name)
}

func (r *registry) DisableHook(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.disableHook(name)
}

// DisableHook disables the hook to be run for the pipeline, deleting previous options.
func DisableHook(name string) {
	defaultRegistry.DisableHook(name)
}

// Encode encodes a hook name and its arguments into a single string.
// This is a convenience function for users of this package that are composing
// hooks.
func Encode(name string, opts []string) string {
	var cfg bytes.Buffer
	w := csv.NewWriter(&cfg)
	// This should never happen since a bytes.Buffer doesn't fail to write.
	if err := w.Write(append([]string{name}, opts...)); err != nil {
		panic(errors.Wrap(err, "error encoding arguments"))
	}
	w.Flush()
	return cfg.String()
}

// Decode decodes a hook name and its arguments from a single string.
// This is a convenience function for users of this package that are composing
// hooks.
func Decode(in string) (string, []string) {
	r := csv.NewReader(strings.NewReader(in))
	s, err := r.Read()
	if err != nil {
		panic(errors.Wrapf(err, "malformed input for decoding: %s", in))
	}
	return s[0], s[1:]
}
