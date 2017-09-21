// Package beamx is a convenience package for beam.
package beamx

import (
	"context"
	"flag"

	"github.com/apache/beam/sdks/go/pkg/beam"
	// The imports here are for the side effect of runner registration.
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/textio/gcs"
	_ "github.com/apache/beam/sdks/go/pkg/beam/io/textio/local"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/dot"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/local"
)

var runner = flag.String("runner", "local", "Pipeline runner.")

// Run invokes beam.Run with the runner supplied by the flag "runner". It
// defaults to the local runner, but all beam-distributed runners and textio
// filesystems are implicitly registered.
func Run(ctx context.Context, p *beam.Pipeline) error {
	return beam.Run(ctx, *runner, p)
}
