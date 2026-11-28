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

package beam

import (
	"reflect"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

type wfnTsOnly struct{}

func (f *wfnTsOnly) AssignWindows(typex.EventTime) []typex.Window { return nil }

type wfnInt64 struct{}

func (f *wfnInt64) AssignWindows(typex.EventTime, int64) []typex.Window { return nil }

type wfnAny struct{}

func (f *wfnAny) AssignWindows(typex.EventTime, any) []typex.Window { return nil }

type wfnKV struct{}

func (f *wfnKV) AssignWindows(typex.EventTime, string, int64) []typex.Window { return nil }

type wfnBytes struct{}

func (f *wfnBytes) AssignWindows(typex.EventTime, []byte) []typex.Window { return nil }

func init() {
	window.RegisterWindowFn[*wfnTsOnly]()
	window.RegisterWindowFn[*wfnInt64]()
	window.RegisterWindowFn[*wfnAny]()
	window.RegisterWindowFn[*wfnKV]()
	window.RegisterWindowFn[*wfnBytes]()
}

func TestValidateWindowFnElements(t *testing.T) {
	int64T := typex.New(reflect.TypeFor[int64]())
	stringT := typex.New(reflect.TypeFor[string]())
	kvT := typex.NewKV(stringT, int64T)

	tests := []struct {
		name    string
		wfn     *window.Fn
		col     typex.FullType
		wantErr bool
	}{
		{"built-in ignores shape", window.NewFixedWindows(time.Second), kvT, false},
		{"timestamp only ignores shape", window.NewCustom(&wfnTsOnly{}), kvT, false},
		{"single element matches", window.NewCustom(&wfnInt64{}), int64T, false},
		{"any element accepts anything", window.NewCustom(&wfnAny{}), stringT, false},
		{"kv matches", window.NewCustom(&wfnKV{}), kvT, false},
		{"single element type mismatch", window.NewCustom(&wfnInt64{}), stringT, true},
		{"single element against kv", window.NewCustom(&wfnInt64{}), kvT, true},
		{"kv against single element", window.NewCustom(&wfnKV{}), int64T, true},
		{"kv component mismatch", window.NewCustom(&wfnKV{}), typex.NewKV(int64T, int64T), true},
		{"element aware against cogbk", window.NewCustom(&wfnInt64{}), typex.NewCoGBK(stringT, int64T), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateWindowFnElements(tc.wfn, tc.col)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateWindowFnElements(%v, %v) error = %v, want error presence %v", tc.wfn, tc.col, err, tc.wantErr)
			}
		})
	}
}

// TestTryWindowIntoValidatesElements checks that TryWindowInto performs the
// element validation rather than leaving the mismatch to fail inside a bundle.
func TestTryWindowIntoValidatesElements(t *testing.T) {
	p := NewPipeline()
	s := p.Root()
	col := Impulse(s) // PCollection<[]byte>

	if _, err := TryWindowInto(s, window.NewCustom(&wfnBytes{}), col); err != nil {
		t.Errorf("TryWindowInto with a matching WindowFn failed: %v", err)
	}
	if _, err := TryWindowInto(s, window.NewCustom(&wfnInt64{}), col); err == nil {
		t.Error("TryWindowInto with a mismatched WindowFn succeeded, want error")
	}
}
