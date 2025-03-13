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

package hooks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/google/go-cmp/cmp"
)

type contextKey string

const (
	initKey   = contextKey("init_key")
	reqKey    = contextKey("req_key")
	initValue = "initValue"
	reqValue  = "reqValue"
)

func initializeHooks(options *runtime.Options) *registry {
	r := newRegistry(options)
	r.RegisterHook("test", func([]string) Hook {
		return Hook{
			Init: func(ctx context.Context) (context.Context, error) {
				return context.WithValue(ctx, initKey, initValue), nil
			},
			Req: func(ctx context.Context, req *fnpb.InstructionRequest) (context.Context, error) {
				return context.WithValue(ctx, reqKey, reqValue), nil
			},
		}
	})
	r.EnableHook("test")
	r.SerializeHooksToOptions()
	r.DeserializeHooksFromOptions(context.Background())
	return r
}

func TestInitContextPropagation(t *testing.T) {
	r := initializeHooks(runtime.NewOptions())
	ctx := context.Background()
	var err error

	ctx, err = r.RunInitHooks(ctx)
	if err != nil {
		t.Errorf("got %v error, wanted no error", err)
	}

	if got, want := ctx.Value(initKey), initValue; got != want {
		t.Errorf("RunInitHooks context.Value: got %s, wanted %s", got, want)
	}
}

func TestRequestContextPropagation(t *testing.T) {
	r := initializeHooks(runtime.NewOptions())
	ctx := context.Background()

	ctx = r.RunRequestHooks(ctx, nil)
	if got, want := ctx.Value(reqKey), reqValue; got != want {
		t.Errorf("RunRequestHooks context.Value: got %s, wanted %s", got, want)
	}
}

// TestConcurrentWrites tests if the concurrent writes are handled properly.
// It uses go routines to test this on sample hook 'google_logging'.
func TestConcurrentWrites(t *testing.T) {
	r := initializeHooks(runtime.NewOptions())
	hf := func(opts []string) Hook {
		return Hook{
			Req: func(ctx context.Context, req *fnpb.InstructionRequest) (context.Context, error) {
				return ctx, nil
			},
		}
	}
	r.RegisterHook("google_logging", hf)

	ch := make(chan struct{})
	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch:
					// When the channel is closed, exit.
					return
				default:
					if got, want := r.EnableHook("google_logging"), error(nil); got != want {
						t.Errorf("Got %s, wanted %s", got, want)
					}
				}
			}
		}()
	}
	// Let the goroutines execute for 5 seconds and then close the channel.
	time.Sleep(time.Second * 5)
	close(ch)
	// Wait for all goroutines to exit properly.
	wg.Wait()
}

func TestHookOrder(t *testing.T) {
	r := newRegistry(runtime.NewOptions())

	want := []string{"a", "d", "b"}
	var got []string
	hf := func(args []string) Hook {
		return Hook{
			Resp: func(context.Context, *fnpb.InstructionRequest, *fnpb.InstructionResponse) error {
				got = append(got, args...)
				return nil
			},
		}
	}

	r.RegisterHook("a", hf)
	r.RegisterHook("b", hf)
	r.RegisterHook("c", hf)
	r.RegisterHook("d", hf)

	r.EnableHook("a", "bad")
	r.EnableHook("a", "a") // override previous option
	r.EnableHook("b", "not here")
	// c never enabled.
	r.EnableHook("d", "d")

	r.DisableHook("b")
	r.EnableHook("b", "b")

	r.SerializeHooksToOptions()
	ctx := context.Background()
	r.DeserializeHooksFromOptions(ctx)

	r.RunResponseHooks(ctx, nil, nil)
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("bad hook ordering: got %v, want %v; diff:\n %v", got, want, d)
	}
}

func TestEncodeDecode(t *testing.T) {
	name, args := "test", []string{"one", "two", "three"}
	data := Encode(name, args)
	gotName, gotArgs := Decode(data)
	if got, want := gotName, name; got != want {
		t.Errorf("Decode(Encode(%v, %v)) = %v, %v", name, args, gotName, gotArgs)
	}
	if !cmp.Equal(args, gotArgs) {
		t.Errorf("Decode(Encode(%v, %v)) = %v, %v", name, args, gotName, gotArgs)
	}
}
