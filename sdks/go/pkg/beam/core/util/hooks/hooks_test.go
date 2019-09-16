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
	"testing"

	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

type contextKey string

const (
	initKey   = contextKey("init_key")
	reqKey    = contextKey("req_key")
	initValue = "initValue"
	reqValue  = "reqValue"
)

func initializeHooks() {
	activeHooks["test"] = Hook{
		Init: func(ctx context.Context) (context.Context, error) {
			return context.WithValue(ctx, initKey, initValue), nil
		},
		Req: func(ctx context.Context, req *fnpb.InstructionRequest) (context.Context, error) {
			return context.WithValue(ctx, reqKey, reqValue), nil
		},
	}
}

func TestInitContextPropagation(t *testing.T) {
	initializeHooks()
	ctx := context.Background()
	var err error

	expected := initValue
	ctx, err = RunInitHooks(ctx)
	if err != nil {
		t.Errorf("got %v error, wanted no error", err)
	}
	actual := ctx.Value(initKey)
	if actual != expected {
		t.Errorf("Got %s, wanted %s", actual, expected)
	}
}

func TestRequestContextPropagation(t *testing.T) {
	initializeHooks()
	ctx := context.Background()

	expected := reqValue
	ctx = RunRequestHooks(ctx, nil)
	actual := ctx.Value(reqKey)
	if actual != expected {
		t.Errorf("Got %s, wanted %s", actual, expected)
	}
}
