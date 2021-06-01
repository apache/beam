package regression

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/go/test/integration"

	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/flink"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/spark"
)

func TestLPErrorPipeline(t *testing.T) {
	integration.CheckFilters(t)

	pipeline, s := beam.NewPipelineWithRoot()
	want := beam.CreateList(s, []int{0})
	got := LPErrorPipeline(s)
	passert.Equals(s, got, want)

	ptest.RunAndValidate(t, pipeline)
}
