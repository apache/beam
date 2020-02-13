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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

var (
	hookRegistry = make(map[string]HookFactory)
	enabledHooks = make(map[string][]string)
	activeHooks  = make(map[string]Hook)
)

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

// RegisterHook registers a Hook for the
// supplied identifier.
func RegisterHook(name string, h HookFactory) {
	hookRegistry[name] = h
}

// RunInitHooks runs the init hooks.
func RunInitHooks(ctx context.Context) (context.Context, error) {
	// If an init hook fails to complete, the invariants of the
	// system are compromised and we can't run a workflow.
	// The hooks can run in any order. They should not be
	// interdependent or interfere with each other.
	for _, h := range activeHooks {
		if h.Init != nil {
			var err error
			if ctx, err = h.Init(ctx); err != nil {
				return ctx, err
			}
		}
	}
	return ctx, nil
}

// RequestHook is called when handling a FnAPI instruction. It can return an updated
// context to pass additional information to downstream callers, or return the
// original context provided.
type RequestHook func(context.Context, *fnpb.InstructionRequest) (context.Context, error)

// RunRequestHooks runs the hooks that handle a FnAPI request.
func RunRequestHooks(ctx context.Context, req *fnpb.InstructionRequest) context.Context {
	// The request hooks should not modify the request.
	for n, h := range activeHooks {
		if h.Req != nil {
			var err error
			if ctx, err = h.Req(ctx, req); err != nil {
				log.Infof(ctx, "request hook %s failed: %v", n, err)
			}
		}
	}
	return ctx
}

// ResponseHook is called when sending a FnAPI instruction response.
type ResponseHook func(context.Context, *fnpb.InstructionRequest, *fnpb.InstructionResponse) error

// RunResponseHooks runs the hooks that handle a FnAPI response.
func RunResponseHooks(ctx context.Context, req *fnpb.InstructionRequest, resp *fnpb.InstructionResponse) {
	for n, h := range activeHooks {
		if h.Resp != nil {
			if err := h.Resp(ctx, req, resp); err != nil {
				log.Infof(ctx, "response hook %s failed: %v", n, err)
			}
		}
	}
}

// SerializeHooksToOptions serializes the activated hooks and their configuration into a JSON string
// that can be deserialized later by the runner.
func SerializeHooksToOptions() {
	data, err := json.Marshal(enabledHooks)
	if err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrap(err, "Couldn't serialize hooks"))
	}
	runtime.GlobalOptions.Set("hooks", string(data))
}

// DeserializeHooksFromOptions extracts the hook configuration information from the options and configures
// the hooks with the supplied options.
func DeserializeHooksFromOptions(ctx context.Context) {
	cfg := runtime.GlobalOptions.Get("hooks")
	if cfg == "" {
		log.Warn(ctx, "SerializeHooksToOptions was never called. No hooks enabled")
		return
	}
	if err := json.Unmarshal([]byte(cfg), &enabledHooks); err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(errors.Wrapf(err, "DeserializeHooks failed on input %q", cfg))
	}

	for h, opts := range enabledHooks {
		activeHooks[h] = hookRegistry[h](opts)
	}
}

// EnableHook enables the hook to be run for the pipline. It will be
// receive the supplied args when the pipeline executes. It is safe
// to enable the same hook with different options, as this is necessary
// if a hook wants to compose behavior.
func EnableHook(name string, args ...string) error {
	if _, ok := hookRegistry[name]; !ok {
		return errors.Errorf("EnableHook: hook %s not found", name)
	}
	enabledHooks[name] = args
	return nil
}

// IsEnabled returns true and the registered options if the hook is
// already enabled.
func IsEnabled(name string) (bool, []string) {
	opts, ok := enabledHooks[name]
	return ok, opts
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
