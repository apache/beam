package primitives

import (
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/periodic"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestTimers_EventTime_Bounded(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TimersEventTime(beam.Impulse))
}

func TestTimers_EventTime_Unbounded(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TimersEventTime(func(s beam.Scope) beam.PCollection {
		now := time.Now()
		return periodic.Impulse(s, now, now.Add(10*time.Second), 0, false)
	}))
}

// TODO(https://github.com/apache/beam/issues/29772): Add ProcessingTime Timer tests.
