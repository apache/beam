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

//lint:file-ignore ST1008 test cases with error returns out of place are intended

package funcx

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/state"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

type foo struct {
	i int
}

type nonConcreteType struct {
	Ghi func(string) string
}

type illegalEmitType struct {
	Abc string
	Def nonConcreteType
}

func (m foo) Do(context.Context, int, string) (string, int, error) {
	return "", m.i, nil
}

func TestNew(t *testing.T) {
	tests := []struct {
		Name  string
		Fn    any
		Param []FnParamKind
		Ret   []ReturnKind
		Err   error
	}{
		{
			Name: "niladic",
			Fn:   func() {},
		},
		{
			Name: "good1",
			Fn: func(context.Context, int, string) (typex.EventTime, string, int, error) {
				return mtime.ZeroTimestamp, "", 0, nil
			},
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetEventTime, RetValue, RetValue, RetError},
		},
		{
			Name:  "good2",
			Fn:    func(int, func(*int) bool, func(*int, *string) bool) {},
			Param: []FnParamKind{FnValue, FnIter, FnIter},
		},
		{
			Name: "good3",
			Fn: func(func(int, int), func(typex.EventTime, int, int), func(string), func(typex.EventTime, string)) {
			},
			Param: []FnParamKind{FnEmit, FnEmit, FnEmit, FnEmit},
		},
		{
			Name:  "good4",
			Fn:    func(typex.EventTime, reflect.Type, []byte) {},
			Param: []FnParamKind{FnEventTime, FnType, FnValue},
		},
		{
			Name:  "good5",
			Fn:    func(typex.Window, typex.EventTime, reflect.Type, []byte) {},
			Param: []FnParamKind{FnWindow, FnEventTime, FnType, FnValue},
		},
		{
			Name:  "good6",
			Fn:    func(int, func(string) func(*int) bool) {},
			Param: []FnParamKind{FnValue, FnMultiMap},
		},
		{
			Name:  "good7",
			Fn:    func(typex.PaneInfo, typex.Window, typex.EventTime, reflect.Type, []byte) {},
			Param: []FnParamKind{FnPane, FnWindow, FnEventTime, FnType, FnValue},
		},
		{
			Name:  "good8",
			Fn:    func(typex.PaneInfo, typex.Window, typex.EventTime, reflect.Type, typex.BundleFinalization, []byte) {},
			Param: []FnParamKind{FnPane, FnWindow, FnEventTime, FnType, FnBundleFinalization, FnValue},
		},
		{
			Name:  "good9",
			Fn:    func(typex.PaneInfo, typex.Window, typex.EventTime, sdf.WatermarkEstimator, reflect.Type, []byte) {},
			Param: []FnParamKind{FnPane, FnWindow, FnEventTime, FnWatermarkEstimator, FnType, FnValue},
		},
		{
			Name:  "good10",
			Fn:    func(sdf.RTracker, state.Provider, []byte) {},
			Param: []FnParamKind{FnRTracker, FnStateProvider, FnValue},
		},
		{
			Name:  "good11",
			Fn:    func(typex.PaneInfo, typex.Window, typex.EventTime, state.Provider, timers.Provider, []byte) {},
			Param: []FnParamKind{FnPane, FnWindow, FnEventTime, FnStateProvider, FnTimerProvider, FnValue},
		},
		{
			Name:  "good-method",
			Fn:    foo{1}.Do,
			Param: []FnParamKind{FnContext, FnValue, FnValue},
			Ret:   []ReturnKind{RetValue, RetValue, RetError},
		},
		{
			Name:  "no inputs, no RetOutput",
			Fn:    func(context.Context, typex.EventTime, reflect.Type, func(int)) error { return nil },
			Param: []FnParamKind{FnContext, FnEventTime, FnType, FnEmit},
			Ret:   []ReturnKind{RetError},
		},
		{
			Name:  "sdf",
			Fn:    func(sdf.RTracker, func(int)) (sdf.ProcessContinuation, error) { return nil, nil },
			Param: []FnParamKind{FnRTracker, FnEmit},
			Ret:   []ReturnKind{RetProcessContinuation, RetError},
		},
		{
			Name: "errContextParam: after input",
			Fn:   func(string, context.Context) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after output",
			Fn:   func(string, func(string), context.Context) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after Window",
			Fn:   func(typex.Window, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after EventTime",
			Fn:   func(typex.EventTime, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: after reflect.Type",
			Fn:   func(reflect.Type, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errContextParam: multiple context",
			Fn:   func(context.Context, context.Context, int) {},
			Err:  errContextParam,
		},
		{
			Name: "errPaneParam: after Window",
			Fn:   func(typex.Window, typex.PaneInfo, int) {},
			Err:  errPaneParamPrecedence,
		},
		{
			Name: "errWindowParamPrecedence: after EventTime",
			Fn: func(typex.EventTime, typex.Window, int) {
			},
			Err: errWindowParamPrecedence,
		},
		{
			Name: "errEventTimeParamPrecedence: after value",
			Fn: func(int, typex.EventTime) {
			},
			Err: errEventTimeParamPrecedence,
		},
		{
			Name: "errEventTimeParamPrecedence: after reflect type",
			Fn: func(reflect.Type, typex.EventTime, int) {
			},
			Err: errEventTimeParamPrecedence,
		},
		{
			Name: "errReflectTypePrecedence: after value",
			Fn: func(int, reflect.Type) {
			},
			Err: errReflectTypePrecedence,
		},
		{
			Name: "errReflectTypePrecedence: after reflect type",
			Fn: func(reflect.Type, reflect.Type, int) {
			},
			Err: errReflectTypePrecedence,
		},
		{
			Name: "errReflectTypePrecedence: after bundle finalizer",
			Fn:   func(typex.PaneInfo, typex.Window, typex.EventTime, typex.BundleFinalization, reflect.Type, []byte) {},
			Err:  errReflectTypePrecedence,
		},
		{
			Name: "errEventTimeParamPrecedence: after watermark estimator",
			Fn:   func(typex.PaneInfo, typex.Window, sdf.WatermarkEstimator, typex.EventTime, reflect.Type, []byte) {},
			Err:  errEventTimeParamPrecedence,
		},
		{
			Name: "errInputPrecedence- Iter before after output",
			Fn:   func(int, func(int), func(*int) bool, func(*int, *string) bool) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errInputPrecedence- ReIter before after output",
			Fn:   func(int, func(int), func() func(*int) bool) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errInputPrecedence- StateProvider before RTracker",
			Fn:   func(state.Provider, sdf.RTracker, int) {},
			Err:  errRTrackerPrecedence,
		},
		{
			Name: "errInputPrecedence- StateProvider after output",
			Fn:   func(int, state.Provider) {},
			Err:  errStateProviderPrecedence,
		},
		{
			Name: "errInputPrecedence- TimerProvider before RTracker",
			Fn:   func(timers.Provider, sdf.RTracker, int) {},
			Err:  errRTrackerPrecedence,
		},
		{
			Name: "errInputPrecedence- TimerProvider after output",
			Fn:   func(int, timers.Provider) {},
			Err:  errTimerProviderPrecedence,
		},
		{
			Name: "errInputPrecedence- TimerProvider before StateProvider",
			Fn:   func(int, timers.Provider, state.Provider) {},
			Err:  errTimerProviderPrecedence,
		},
		{
			Name: "errInputPrecedence- input after output",
			Fn:   func(int, func(int), int) {},
			Err:  errInputPrecedence,
		},
		{
			Name: "errErrorPrecedence - first",
			Fn: func() (error, string) {
				return nil, ""
			},
			Err: errErrorPrecedence,
		},
		{
			Name: "errErrorPrecedence - second",
			Fn: func() (typex.EventTime, error, string) {
				return mtime.ZeroTimestamp, nil, ""
			},
			Err: errErrorPrecedence,
		},
		{
			Name: "errBundleFinalizationPrecedence",
			Fn:   func(typex.PaneInfo, typex.Window, typex.EventTime, reflect.Type, []byte, typex.BundleFinalization) {},
			Err:  errBundleFinalizationPrecedence,
		},
		{
			Name: "errWatermarkEstimatorParamPrecedence",
			Fn:   func(typex.PaneInfo, typex.Window, typex.EventTime, reflect.Type, sdf.WatermarkEstimator) {},
			Err:  errWatermarkEstimatorParamPrecedence,
		},
		{
			Name: "errEventTimeRetPrecedence",
			Fn: func() (string, typex.EventTime) {
				return "", mtime.ZeroTimestamp
			},
			Err: errEventTimeRetPrecedence,
		},
		{
			Name: "errProcessContinuationPrecedence",
			Fn: func() (string, sdf.ProcessContinuation, int, error) {
				return "", nil, 0, nil
			},
			Err: errProcessContinuationPrecedence,
		},
		{
			Name: "errIllegalParametersInEmit - malformed emit struct",
			Fn:   func(context.Context, typex.EventTime, reflect.Type, func(nonConcreteType)) error { return nil },
			Err:  errors.New(errIllegalParametersInEmit),
		},
		{
			Name: "errIllegalParametersInEmit - malformed emit nested in struct",
			Fn:   func(context.Context, typex.EventTime, reflect.Type, func(illegalEmitType)) error { return nil },
			Err:  errors.New(errIllegalParametersInEmit),
		},
		{
			Name: "errIllegalParametersInEmit - malformed emit nested in map value",
			Fn: func(context.Context, typex.EventTime, reflect.Type, func(map[string]nonConcreteType)) error {
				return nil
			},
			Err: errors.New(errIllegalParametersInEmit),
		},
		{
			Name: "errIllegalParametersInEmit - malformed emit nested in slice",
			Fn: func(context.Context, typex.EventTime, reflect.Type, func([]nonConcreteType)) error {
				return nil
			},
			Err: errors.New(errIllegalParametersInEmit),
		},
		{
			Name: "errIllegalParametersInIter - malformed Iter",
			Fn:   func(int, func(*nonConcreteType) bool, func(*int, *string) bool) {},
			Err:  errors.New(errIllegalParametersInIter),
		},
		{
			Name: "errIllegalParametersInIter - malformed ReIter",
			Fn:   func(int, func() func(*nonConcreteType) bool, func(*int, *string) bool) {},
			Err:  errors.New(errIllegalParametersInReIter),
		},
		{
			Name: "errIllegalParametersInMultiMap - malformed MultiMap",
			Fn:   func(int, func(string) func(*nonConcreteType) bool) {},
			Err:  errors.New(errIllegalParametersInMultiMap),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			u, err := New(reflectx.MakeFunc(test.Fn))
			if err != nil {
				if test.Err == nil {
					t.Fatalf("Expected test.Err to be set; got: New(%v) failed: %v", test.Fn, err)
				}
				if u != nil {
					t.Errorf("New(%v) failed, and returned non nil: %v, %v", test.Fn, u, err)
				}
				if strings.Contains(err.Error(), test.Err.Error()) {
					return // Received the expected error.
				}
				t.Fatalf("New(%v) failed: %v;\n\twant err to contain: \"%v\"", test.Fn, err, test.Err)
			}

			param := projectParamKind(u)
			if !reflect.DeepEqual(param, test.Param) {
				t.Errorf("New(%v).Param = %v, want %v", test.Fn, param, test.Param)
			}
			ret := projectReturnKind(u)
			if !reflect.DeepEqual(ret, test.Ret) {
				t.Errorf("New(%v).Ret = %v, want %v", test.Fn, ret, test.Ret)
			}
		})
	}
}

func TestEmits(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Num    int
		Exists bool
	}{
		{
			Name:   "no params",
			Params: []FnParamKind{},
			Pos:    -1,
			Num:    0,
			Exists: false,
		},
		{
			Name:   "no emits",
			Params: []FnParamKind{FnContext, FnEventTime, FnType, FnValue},
			Pos:    -1,
			Num:    0,
			Exists: false,
		},
		{
			Name:   "single emit",
			Params: []FnParamKind{FnValue, FnEmit},
			Pos:    1,
			Num:    1,
			Exists: true,
		},
		{
			Name:   "multiple emits",
			Params: []FnParamKind{FnValue, FnEmit, FnEmit, FnEmit},
			Pos:    1,
			Num:    3,
			Exists: true,
		},
		{
			Name:   "multiple emits 2",
			Params: []FnParamKind{FnValue, FnEmit, FnEmit, FnEmit, FnValue},
			Pos:    1,
			Num:    3,
			Exists: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for Emits function.
			pos, num, exists := fn.Emits()
			if exists != test.Exists {
				t.Errorf("Emits() exists: got %v, want %v", exists, test.Exists)
			}
			if num != test.Num {
				t.Errorf("Emits() num: got %v, want %v", num, test.Num)
			}
			if pos != test.Pos {
				t.Errorf("Emits() pos: got %v, want %v", pos, test.Pos)
			}
		})
	}
}

func TestPane(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "pane input",
			Params: []FnParamKind{FnContext, FnPane},
			Pos:    1,
			Exists: true,
		},
		{
			Name:   "no pane input",
			Params: []FnParamKind{FnContext, FnEventTime},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for pane function.
			pos, exists := fn.Pane()
			if exists != test.Exists {
				t.Errorf("Pane(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("Pane(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestWindow(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "window input",
			Params: []FnParamKind{FnContext, FnWindow},
			Pos:    1,
			Exists: true,
		},
		{
			Name:   "no window input",
			Params: []FnParamKind{FnContext, FnEventTime},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for Window function.
			pos, exists := fn.Window()
			if exists != test.Exists {
				t.Errorf("Window(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("Window(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestBundleFinalization(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "bundleFinalization input",
			Params: []FnParamKind{FnContext, FnBundleFinalization},
			Pos:    1,
			Exists: true,
		},
		{
			Name:   "no bundleFinalization input",
			Params: []FnParamKind{FnContext, FnEventTime},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for BundleFinalization function.
			pos, exists := fn.BundleFinalization()
			if exists != test.Exists {
				t.Errorf("BundleFinalization(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("BundleFinalization(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestWatermarkEstimator(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "watermarkEstimator input",
			Params: []FnParamKind{FnContext, FnWatermarkEstimator},
			Pos:    1,
			Exists: true,
		},
		{
			Name:   "no watermarkEstimator input",
			Params: []FnParamKind{FnContext, FnEventTime},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for WatermarkEstimator function.
			pos, exists := fn.WatermarkEstimator()
			if exists != test.Exists {
				t.Errorf("WatermarkEstimator(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("WatermarkEstimator(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestTimerProvider(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "TimerProvider input",
			Params: []FnParamKind{FnContext, FnWindow, FnEventTime, FnTimerProvider},
			Pos:    3,
			Exists: true,
		},
		{
			Name:   "no TimerProvider input",
			Params: []FnParamKind{FnContext, FnWindow, FnEventTime},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for TimerProvider function.
			pos, exists := fn.TimerProvider()
			if exists != test.Exists {
				t.Errorf("TimerProvider(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("TimerProvider(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestStateProvider(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Exists bool
	}{
		{
			Name:   "StateProvider input",
			Params: []FnParamKind{FnContext, FnWindow, FnStateProvider},
			Pos:    2,
			Exists: true,
		},
		{
			Name:   "no StateProvider input",
			Params: []FnParamKind{FnContext, FnWindow},
			Pos:    -1,
			Exists: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for StateProvider function.
			pos, exists := fn.StateProvider()
			if exists != test.Exists {
				t.Errorf("StateProvider(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if pos != test.Pos {
				t.Errorf("StateProvider(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func TestInputs(t *testing.T) {
	tests := []struct {
		Name   string
		Params []FnParamKind
		Pos    int
		Num    int
		Exists bool
	}{
		{
			Name:   "no params",
			Params: []FnParamKind{},
			Pos:    -1,
			Num:    0,
			Exists: false,
		},
		{
			Name:   "no inputs",
			Params: []FnParamKind{FnContext, FnEventTime, FnType, FnEmit},
			Pos:    -1,
			Num:    0,
			Exists: false,
		},
		{
			Name:   "no main input",
			Params: []FnParamKind{FnContext, FnIter, FnReIter, FnEmit},
			Pos:    1,
			Num:    2,
			Exists: true,
		},
		{
			Name:   "single input",
			Params: []FnParamKind{FnContext, FnValue},
			Pos:    1,
			Num:    1,
			Exists: true,
		},
		{
			Name:   "multiple inputs",
			Params: []FnParamKind{FnContext, FnValue, FnIter, FnReIter},
			Pos:    1,
			Num:    3,
			Exists: true,
		},
		{
			Name:   "multiple inputs 2",
			Params: []FnParamKind{FnContext, FnValue, FnIter, FnValue, FnReIter, FnEmit},
			Pos:    1,
			Num:    4,
			Exists: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			// Create a Fn with a filled params list.
			params := make([]FnParam, len(test.Params))
			for i, kind := range test.Params {
				params[i].Kind = kind
				params[i].T = nil
			}
			fn := &Fn{Param: params}

			// Validate we get expected results for Inputs function.
			pos, num, exists := fn.Inputs()
			if exists != test.Exists {
				t.Errorf("Inputs(%v) - exists: got %v, want %v", params, exists, test.Exists)
			}
			if num != test.Num {
				t.Errorf("Inputs(%v) - num: got %v, want %v", params, num, test.Num)
			}
			if pos != test.Pos {
				t.Errorf("Inputs(%v) - pos: got %v, want %v", params, pos, test.Pos)
			}
		})
	}
}

func projectParamKind(u *Fn) []FnParamKind {
	var ret []FnParamKind
	for _, p := range u.Param {
		ret = append(ret, p.Kind)
	}
	return ret
}

func projectReturnKind(u *Fn) []ReturnKind {
	var ret []ReturnKind
	for _, p := range u.Ret {
		ret = append(ret, p.Kind)
	}
	return ret
}
