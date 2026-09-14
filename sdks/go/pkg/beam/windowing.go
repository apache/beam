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
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

type WindowIntoOption interface {
	windowIntoOption()
}

type windowTrigger struct {
	trigger trigger.Trigger
}

func (t windowTrigger) windowIntoOption() {}

// Trigger applies the given trigger to the window.
func Trigger(tr trigger.Trigger) WindowIntoOption {
	return windowTrigger{trigger: tr}
}

type accumulationMode struct {
	mode window.AccumulationMode
}

func (m accumulationMode) windowIntoOption() {}

// PanesAccumulate applies an Accumulating AccumulationMode to the window.
// After a pane fires, already processed elements will accumulate and
// elements will be repeated in subseqent firings for the window.
func PanesAccumulate() WindowIntoOption {
	return accumulationMode{mode: window.Accumulating}
}

// PanesDiscard applies a Discarding AccumulationMode to the window.
// After a pane fires, already processed elements will be discarded
// and not included in later firings for the window.
func PanesDiscard() WindowIntoOption {
	return accumulationMode{mode: window.Discarding}
}

type allowedLateness struct {
	delay time.Duration
}

func (m allowedLateness) windowIntoOption() {}

// AllowedLateness configures for how long data may arrive after the end of a window.
func AllowedLateness(delay time.Duration) WindowIntoOption {
	return allowedLateness{delay: delay}
}

// WindowInto applies the windowing strategy to each element.
func WindowInto(s Scope, ws *window.Fn, col PCollection, opts ...WindowIntoOption) PCollection {
	return Must(TryWindowInto(s, ws, col, opts...))
}

// TryWindowInto attempts to insert a WindowInto transform.
func TryWindowInto(s Scope, wfn *window.Fn, col PCollection, opts ...WindowIntoOption) (PCollection, error) {
	if !s.IsValid() {
		return PCollection{}, errors.New("invalid scope")
	}
	if !col.IsValid() {
		return PCollection{}, errors.New("invalid input pcollection")
	}
	if err := validateWindowFnElements(wfn, col.Type()); err != nil {
		return PCollection{}, err
	}
	ws := window.WindowingStrategy{Fn: wfn, Trigger: trigger.DefaultTrigger{}}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case windowTrigger:
			// TODO(BEAM-3304): call validation on trigger construction here
			// so local errors can be returned to the user in their pipeline
			// context instead of at pipeline translation time.
			ws.Trigger = opt.trigger
		case accumulationMode:
			ws.AccumulationMode = opt.mode
		case allowedLateness:
			ws.AllowedLateness = int(opt.delay / time.Millisecond)
		default:
			panic(fmt.Sprintf("Unknown WindowInto option type: %T: %v", opt, opt))
		}
	}

	edge := graph.NewWindowInto(s.real, s.scope, &ws, col.n)
	ret := PCollection{edge.Output[0].To}
	return ret, nil
}

// validateWindowFnElements rejects a custom WindowFn whose AssignWindows
// element parameters cannot receive the PCollection's element. WindowInto binds
// the element the way a DoFn receives its main input, so a KV supplies a key
// and a value while anything else supplies a single element.
func validateWindowFnElements(wfn *window.Fn, t typex.FullType) error {
	if wfn.Kind != window.CustomWindows {
		return nil
	}
	elems, ok := window.LookupWindowFn(reflectx.SkipPtr(reflect.TypeOf(wfn.CustomFn)))
	if !ok || len(elems) == 0 {
		return nil
	}

	var supplied []typex.FullType
	switch {
	case typex.IsKV(t):
		supplied = t.Components()
	case t.Class() == typex.Composite:
		return errors.Errorf("element-aware custom WindowFn %T cannot window a %v PCollection", wfn.CustomFn, t)
	default:
		supplied = []typex.FullType{t}
	}

	if len(elems) != len(supplied) {
		return errors.Errorf("custom WindowFn %T takes %v element parameter(s) in AssignWindows, but a %v PCollection supplies %v", wfn.CustomFn, len(elems), t, len(supplied))
	}
	for i, elem := range elems {
		if supplied[i].Class() == typex.Universal {
			continue // Not bound yet, so nothing to check here.
		}
		if !supplied[i].Type().AssignableTo(elem) {
			return errors.Errorf("custom WindowFn %T AssignWindows element parameter %v is %v, which cannot receive %v from a %v PCollection", wfn.CustomFn, i+1, elem, supplied[i].Type(), t)
		}
	}
	return nil
}
