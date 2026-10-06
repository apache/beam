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

package exec

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

// This file covers how metrics incremented from a DoFn's Setup method reach the
// metrics store of the bundle being processed.
//
// Setup runs once per DoFn node before any element of a bundle is processed
// (ParDo.Up is invoked from Plan.Execute for the first bundle the plan
// executes), which makes it the natural place to count one-time work: clients
// or connection pools opened, files read, state deserialized. Pipeline authors
// expect those counters to be reported to the runner just like the counters
// they increment in ProcessElement.
//
// Metrics in the Go SDK are scoped to a context rather than to a global
// registry: an increment records into the metrics store carried by the context
// it is given, and a runner only reads back the store of the bundle that is
// executing. Whether a metric incremented in Setup survives therefore depends
// on the context Setup receives and on the context the DoFn chooses to use:
//
//   - ParDo.Up derives setupCtx := metrics.SetPTransformID(ctx, n.PID) from the
//     incoming bundle context and invokes Setup with it. A DoFn that implements
//     the context-aware Setup(ctx context.Context) and increments with that ctx
//     has its metrics attributed to the executing PTransform and visible in the
//     bundle's metrics store.
//   - That setup context is derived fresh on each Up call rather than cached,
//     since it belongs to the bundle that triggered Setup. Metrics reported
//     from Setup are therefore only recoverable if the DoFn increments using
//     the context it is handed. A DoFn whose Setup takes no arguments cannot
//     observe it and will fall back to a context of its own, such as
//     context.Background(), whose store no runner reads back: those increments
//     are silently lost, which is the bug reported in apache/beam#27038
//     (BEAM-27038).
//
// The two tests below pin down both halves of that contract, so that the lossy
// case stays explicit and deliberate instead of regressing silently.

// setupMetricsCounter is incremented by setupMetricsDoFnWithContext.Setup using
// the context supplied by the framework.
var setupMetricsCounter = metrics.NewCounter("test", "setup_counter_with_ctx")

// setupMetricsCounterWithoutContext is incremented by
// setupMetricsDoFnWithoutContext.Setup using a context that the DoFn creates
// itself, because the context-free Setup signature gives it nothing else to use.
var setupMetricsCounterWithoutContext = metrics.NewCounter("test", "setup_counter_no_ctx")

// setupMetricsDoFnWithContext reports a counter from the context-aware Setup
// variant, so the reported value is expected to be attributed to the DoFn's
// PTransform and to be readable back from the bundle's metrics store.
type setupMetricsDoFnWithContext struct{}

// Setup increments setupMetricsCounter using the framework-provided context, so
// the increment is recorded in the metrics store bound to the executing bundle.
func (fn *setupMetricsDoFnWithContext) Setup(ctx context.Context) {
	setupMetricsCounter.Inc(ctx, 1)
}

func (fn *setupMetricsDoFnWithContext) ProcessElement(v int) int {
	return v
}

// setupMetricsDoFnWithoutContext reports a counter from the context-free Setup
// variant, which has no access to the bundle's context.
type setupMetricsDoFnWithoutContext struct{}

// Setup increments setupMetricsCounterWithoutContext against
// context.Background(). The resulting metric is not part of the bundle's
// metrics, which is the behavior the companion test asserts.
func (fn *setupMetricsDoFnWithoutContext) Setup() {
	setupMetricsCounterWithoutContext.Inc(context.Background(), 1)
}

func (fn *setupMetricsDoFnWithoutContext) ProcessElement(v int) int {
	return v
}

// executeSetupMetricsPlan builds the minimal pipeline FixedRoot -> ParDo ->
// CaptureNode around the supplied DoFn and executes it once as the bundle
// "bundle-1".
//
// It returns the executed ParDo, whose PID is the transform ID that metrics
// reported from the DoFn are expected to be attributed to, and the bundle
// context, which holds the metrics store a runner would read the bundle's
// metrics back from. Every test in this file inspects exactly that store, so
// they all share this setup.
func executeSetupMetricsPlan(t *testing.T, dofn any) (*ParDo, context.Context) {
	t.Helper()

	fn, err := graph.NewDoFn(dofn)
	if err != nil {
		t.Fatalf("invalid DoFn: %v", err)
	}

	g := graph.New()
	nN := g.NewNode(typex.New(reflectx.Int), window.DefaultWindowingStrategy(), true)

	edge, err := graph.NewParDo(g, g.Root(), fn, []*graph.Node{nN}, nil, nil)
	if err != nil {
		t.Fatalf("invalid pardo: %v", err)
	}

	out := &CaptureNode{UID: 1}
	pardo := &ParDo{UID: 2, Fn: edge.DoFn, Inbound: edge.Input, Out: []Node{out}}
	root := &FixedRoot{UID: 3, Elements: makeInput(1, 2, 3), Out: pardo}

	p, err := NewPlan("test-plan", []Unit{root, pardo, out})
	if err != nil {
		t.Fatalf("failed to construct plan: %v", err)
	}

	// In a real runner, SetBundleID is called before Execute.
	ctx := metrics.SetBundleID(context.Background(), "bundle-1")

	if err := p.Execute(ctx, "bundle-1", DataContext{}); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	return pardo, ctx
}

// TestSetupMetricsWithContext verifies that a metric incremented in a DoFn's
// context-aware Setup method is captured, attributed to the executing
// PTransform, and readable from the bundle's metrics store. This is the
// behavior pipeline authors rely on when they count one-time per-bundle work
// alongside their per-element counters, and it is the behavior that must not
// regress for apache/beam#27038.
func TestSetupMetricsWithContext(t *testing.T) {
	pardo, ctx := executeSetupMetricsPlan(t, &setupMetricsDoFnWithContext{})

	store := metrics.GetStore(ctx)
	if store == nil {
		t.Fatal("no metrics store found")
	}

	var found bool
	extractor := metrics.Extractor{
		SumInt64: func(labels metrics.Labels, v int64) {
			if labels.Transform() == pardo.PID && labels.Namespace() == "test" && labels.Name() == "setup_counter_with_ctx" {
				if v != 1 {
					t.Errorf("expected counter value 1, got %v", v)
				}
				found = true
			}
		},
	}

	if err := extractor.ExtractFrom(store); err != nil {
		t.Fatalf("extraction failed: %v", err)
	}

	if !found {
		t.Error("setup_counter_with_ctx metric not found in the bundle's metrics store")
	}
}

// TestSetupMetricsWithoutContext documents the other side of the same contract.
// Setup is invoked with a context carrying the bundle and PTransform IDs, but a
// DoFn whose Setup takes no arguments cannot observe it and increments against
// a context of its own choosing instead. That increment never reaches the
// bundle's metrics store and is dropped when the bundle's metrics are reported,
// which is the loss described in apache/beam#27038. Pinning this behavior keeps
// it visible, so that any future change making these metrics recoverable
// requires a deliberate update of this test.
func TestSetupMetricsWithoutContext(t *testing.T) {
	pardo, ctx := executeSetupMetricsPlan(t, &setupMetricsDoFnWithoutContext{})

	store := metrics.GetStore(ctx)
	if store == nil {
		t.Fatal("no metrics store found")
	}

	var found bool
	extractor := metrics.Extractor{
		SumInt64: func(labels metrics.Labels, v int64) {
			if labels.Transform() == pardo.PID && labels.Namespace() == "test" && labels.Name() == "setup_counter_no_ctx" {
				if v != 1 {
					t.Errorf("expected counter value 1, got %v", v)
				}
				found = true
			}
		},
	}

	if err := extractor.ExtractFrom(store); err != nil {
		t.Fatalf("extraction failed: %v", err)
	}

	if found {
		t.Error("setup_counter_no_ctx metric should not be found in the bundle's metrics store when Setup() does not accept context")
	}
}
