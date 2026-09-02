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

var setupCounterNoCtx = metrics.NewCounter("test", "setup_counter_no_ctx")

type SetupMetricsDoFnNoCtx struct{}

func (fn *SetupMetricsDoFnNoCtx) Setup() {
	setupCounterNoCtx.Inc(context.Background(), 1)
}

func (fn *SetupMetricsDoFnNoCtx) ProcessElement(v int) int {
	return v
}

func TestSetupMetricsNoCtx(t *testing.T) {
	fn, err := graph.NewDoFn(&SetupMetricsDoFnNoCtx{})
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
