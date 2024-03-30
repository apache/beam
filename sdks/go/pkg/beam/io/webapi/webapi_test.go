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
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/google/go-cmp/cmp"
	"reflect"
	"testing"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*mockCaller)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*mockCallerSetupTeardown)(nil)).Elem())
}

func Test_newCallerFnFromHybridIFace(t *testing.T) {
	ctx := context.Background()
	caller := &mockCallerSetupTeardown{}
	fn, err := newCallerFnFromHybridIFace(caller)
	if err != nil {
		t.Fatalf("error newCallerFnFromHybridIFace(%T): %v", caller, err)
	}
	if err = fn.Setup(ctx); err != nil {
		t.Fatalf("error Setup() %v", err)
	}

	wantP := []byte("test payload")
	if fn.iface == nil {
		t.Fatalf("callerFn iface is nil")
	}

	resp, err := fn.iface.Call(ctx, &Request{
		Payload: wantP,
	})

	if err != nil {
		t.Fatalf("error callerFn iface Call() %v", err)
	}

	if diff := cmp.Diff(wantP, resp.Payload); diff != "" {
		t.Fatalf("error Payload mismatch (-want +got):\n%s", diff)
	}

	if err = fn.Teardown(ctx); err != nil {
		t.Fatalf("error Teardown() %v", err)
	}
}

func Test_newCallerFn(t *testing.T) {
	ctx := context.Background()
	caller := &mockCaller{}
	fn, err := newCallerFn(caller)
	if err != nil {
		t.Fatalf("error newCallerFn(%T): %v", caller, err)
	}

	if err = fn.Setup(ctx); err != nil {
		t.Fatalf("error Setup() %v", err)
	}

	wantP := []byte("test payload")
	if fn.iface == nil {
		t.Fatalf("callerFn iface is nil")
	}

	resp, err := fn.iface.Call(ctx, &Request{
		Payload: wantP,
	})

	if err != nil {
		t.Fatalf("error callerFn iface Call() %v", err)
	}

	if diff := cmp.Diff(wantP, resp.Payload); diff != "" {
		t.Fatalf("error Payload mismatch (-want +got):\n%s", diff)
	}

	if err = fn.Teardown(ctx); err != nil {
		t.Fatalf("error Teardown() %v", err)
	}
}

type mockCaller struct{}

func (m *mockCaller) Call(_ context.Context, request *Request) (*Response, error) {
	return &Response{
		Payload: request.Payload,
	}, nil
}

type mockCallerSetupTeardown struct{}

func (m *mockCallerSetupTeardown) Call(ctx context.Context, request *Request) (*Response, error) {
	return &Response{
		Payload: request.Payload,
	}, nil
}

func (m *mockCallerSetupTeardown) Setup(ctx context.Context) error {
	return nil
}

func (m *mockCallerSetupTeardown) Teardown(ctx context.Context) error {
	return nil
}
