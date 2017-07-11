// Package beamx is a convenience package for beam.
package beamx

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/local"
)

var runner = flag.String("runner", "local", "Pipeline runner.")

// Run invokes beam.Run with the runner supplied by the flag "runner". It
// defaults to the local runner, but all beam-distributed runners are
// implicitly registered.
func Run(ctx context.Context, p *beam.Pipeline) error {
	return beam.Run(ctx, *runner, p)
}
