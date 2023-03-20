package periodic

import (
	"context"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func TestSequence(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	sd := SequenceDefinition{
		Interval: time.Second,
		Start:    0,
		End:      time.Minute.Milliseconds(),
	}
	in := beam.Create(s, sd)
	out := Sequence(s, in)
	passert.Count(s, out, "SecondsInMinute", 60)
	beam.Init()
	_, err := prism.Execute(context.Background(), p)
	if err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}

func TestImpulse(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	interval := time.Second
	start := time.Unix(0, 0)
	end := start.Add(time.Minute)
	out := Impulse(s, start, end, interval, false)
	passert.Count(s, out, "SecondsInMinute", 60)
	beam.Init()
	_, err := prism.Execute(context.Background(), p)
	if err != nil {
		t.Fatalf("Failed to execute job: %v", err)
	}
}
