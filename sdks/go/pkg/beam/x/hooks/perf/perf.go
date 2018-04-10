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

// Package perf is to add performance measuring hooks to a runner, such as cpu, or trace profiles.
package perf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/pprof"
	"runtime/trace"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

// CaptureHook is used by the harness to have the runner
// persist a trace record with the supplied name and comment.
// The type of trace can be determined by the prefix of the string.
//
// * prof: A profile compatible with traces produced by runtime/pprof
// * trace: A trace compatible with traces produced by runtime/trace
type CaptureHook func(context.Context, string, io.Reader) error

// CaptureHookFactory creates a CaptureHook from the supplied options.
type CaptureHookFactory func([]string) CaptureHook

var profCaptureHookRegistry = make(map[string]CaptureHookFactory)

var enabledProfCaptureHooks []string

func init() {
	hf := func(opts []string) hooks.Hook {
		enabledProfCaptureHooks = opts
		enabled := len(enabledProfCaptureHooks) > 0
		var cpuProfBuf bytes.Buffer
		return hooks.Hook{
			Req: func(_ context.Context, _ *fnpb.InstructionRequest) error {
				if !enabled {
					return nil
				}
				cpuProfBuf.Reset()
				return pprof.StartCPUProfile(&cpuProfBuf)
			},
			Resp: func(ctx context.Context, req *fnpb.InstructionRequest, _ *fnpb.InstructionResponse) error {
				if !enabled {
					return nil
				}
				pprof.StopCPUProfile()
				for _, h := range enabledProfCaptureHooks {
					name, opts := hooks.Decode(h)
					if err := profCaptureHookRegistry[name](opts)(ctx, fmt.Sprintf("prof%s", req.InstructionId), &cpuProfBuf); err != nil {
						return err
					}
				}
				return nil
			},
		}
	}
	hooks.RegisterHook("prof", hf)

	hf = func(opts []string) hooks.Hook {
		var traceProfBuf bytes.Buffer
		enabledTraceCaptureHooks = opts
		enabled := len(enabledTraceCaptureHooks) > 0
		return hooks.Hook{
			Req: func(_ context.Context, _ *fnpb.InstructionRequest) error {
				if !enabled {
					return nil
				}
				traceProfBuf.Reset()
				return trace.Start(&traceProfBuf)
			},
			Resp: func(ctx context.Context, req *fnpb.InstructionRequest, _ *fnpb.InstructionResponse) error {
				if !enabled {
					return nil
				}
				trace.Stop()
				for _, h := range enabledTraceCaptureHooks {
					name, opts := hooks.Decode(h)
					if err := traceCaptureHookRegistry[name](opts)(ctx, fmt.Sprintf("trace_prof%s", req.InstructionId), &traceProfBuf); err != nil {
						return err
					}
				}
				return nil
			},
		}
	}
	hooks.RegisterHook("trace", hf)
}

// RegisterProfCaptureHook registers a CaptureHookFactory for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterProfCaptureHook(name string, c CaptureHookFactory) {
	if _, exists := profCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterProfCaptureHook: %s registered twice", name))
	}
	profCaptureHookRegistry[name] = c
}

// EnableProfCaptureHook actives a registered profile capture hook for a given pipeline.
func EnableProfCaptureHook(name string, opts ...string) {
	_, exists := profCaptureHookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("EnableProfCaptureHook: %s not registered", name))
	}

	enc := hooks.Encode(name, opts)

	for i, h := range enabledProfCaptureHooks {
		n, _ := hooks.Decode(h)
		if h == n {
			// Rewrite the registration with the current arguments
			enabledProfCaptureHooks[i] = enc
			hooks.EnableHook("prof", enabledProfCaptureHooks...)
			return
		}
	}

	enabledProfCaptureHooks = append(enabledProfCaptureHooks, enc)
	hooks.EnableHook("prof", enabledProfCaptureHooks...)
}

var traceCaptureHookRegistry = make(map[string]CaptureHookFactory)
var enabledTraceCaptureHooks []string

// RegisterTraceCaptureHook registers a CaptureHookFactory for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterTraceCaptureHook(name string, c CaptureHookFactory) {
	if _, exists := traceCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterTraceCaptureHook: %s registered twice", name))
	}
	traceCaptureHookRegistry[name] = c
}

// EnableTraceCaptureHook actives a registered profile capture hook for a given pipeline.
func EnableTraceCaptureHook(name string, opts ...string) {
	if _, exists := traceCaptureHookRegistry[name]; !exists {
		panic(fmt.Sprintf("EnableTraceCaptureHook: %s not registered", name))
	}

	enc := hooks.Encode(name, opts)
	for i, h := range enabledTraceCaptureHooks {
		n, _ := hooks.Decode(h)
		if h == n {
			// Rewrite the registration with the current arguments
			enabledTraceCaptureHooks[i] = enc
			hooks.EnableHook("trace", enabledTraceCaptureHooks...)
			return
		}
	}
	enabledTraceCaptureHooks = append(enabledTraceCaptureHooks, enc)
	hooks.EnableHook("trace", enabledTraceCaptureHooks...)
}
