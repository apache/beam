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

//lint:file-ignore U1000 unused functions/types are for tests

package graphx

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	v1pb "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/v1"
)

func TestEncodeType(t *testing.T) {
	t.Run("NoUnexportedFields", func(t *testing.T) {
		type MyAwesomeType struct {
			ExportedField string
		}
		rt := reflect.TypeOf((*MyAwesomeType)(nil)).Elem()

		pbT, err := encodeType(rt)
		if err != nil {
			t.Fatalf("got error = %v, want nil", err)
		}
		if got, want := pbT.Kind, v1pb.Type_STRUCT; got != want {
			t.Fatalf("got pbT.Kind == %v, want %v", got, want)
		}
	})
	t.Run("UnregisteredWithUnexportedField", func(t *testing.T) {
		type MyProblematicType struct {
			unexportedField string
		}
		rt := reflect.TypeOf((*MyProblematicType)(nil)).Elem()
		pbT, err := encodeType(rt)
		if err == nil {
			t.Fatalf("got type = %v, nil, want unexported field error", pbT)
		}
		if !strings.Contains(err.Error(), "type has unexported field: unexportedField") {
			t.Errorf("expected error about unexported field, got %q", err.Error())
		}
	})
	t.Run("RegisteredWithUnexportedField", func(t *testing.T) {
		type MyRegisteredType struct {
			unexportedField string
		}
		rt := reflect.TypeOf((*MyRegisteredType)(nil)).Elem()
		runtime.RegisterType(rt)
		pbT, err := encodeType(rt)
		if err != nil {
			t.Fatalf("got error = %v, want nil", err)
		}
		if got, want := pbT.Kind, v1pb.Type_EXTERNAL; got != want {
			t.Fatalf("got pbT.Kind == %v, want %v", got, want)
		}
	})
	t.Run("FieldArrayWithoutWrapping", func(t *testing.T) {
		rt := reflect.TypeOf((*[4]int)(nil)).Elem()
		pbT, err := encodeType(rt)
		if err == nil {
			t.Fatalf("got type = %v, nil, want no wrap error", pbT)
		}
		if !strings.Contains(err.Error(), "try to wrap the type as a field in a struct") {
			t.Errorf("expected error about wrapping in a struct, got %q", err.Error())
		}
	})
	t.Run("FieldMapWithoutWrapping", func(t *testing.T) {
		rt := reflect.TypeOf((*map[int]string)(nil)).Elem()
		pbT, err := encodeType(rt)
		if err == nil {
			t.Fatalf("got type = %v, nil, want no wrap error", pbT)
		}
		if !strings.Contains(err.Error(), "try to wrap the type as a field in a struct") {
			t.Errorf("expected error about wrapping in a struct, got %q", err.Error())
		}
	})
}

func TestWindowFnRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		fn   *window.Fn
	}{
		{"global", window.NewGlobalWindows()},
		{"fixed", window.NewFixedWindows(5 * time.Second)},
		{"sliding", window.NewSlidingWindows(1*time.Second, 3*time.Second)},
		{"sessions", window.NewSessions(10 * time.Second)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pb := encodeWindowFn(tc.fn)
			got := decodeWindowFn(pb)
			if !tc.fn.Equals(got) {
				t.Errorf("roundtrip mismatch: got %v, want %v", got, tc.fn)
			}
		})
	}
}

func TestWindowFnRoundTrip_CustomKind(t *testing.T) {
	// Custom WindowFns are serialized via the Beam model proto
	// (FunctionSpec), not the internal v1 proto. The v1 path only
	// preserves the Kind so that EncodeMultiEdge does not fail.
	fn := &window.Fn{Kind: window.CustomWindows}
	pb := encodeWindowFn(fn)
	got := decodeWindowFn(pb)
	if got.Kind != window.CustomWindows {
		t.Errorf("kind mismatch: got %v, want %v", got.Kind, window.CustomWindows)
	}
}
