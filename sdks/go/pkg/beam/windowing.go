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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
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
