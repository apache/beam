package beamexec

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/dot"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/harness"
)

var (
	// The below 4 flags implement the Fn API container contract. Subject to change.
	worker          = flag.Bool("worker", false, "Whether binary is running in worker mode.")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	persistDir      = flag.String("persist_dir", "", "Local semi-persistent directory (required in worker mode).")

	runner = flag.String("runner", "local", "Pipeline runner (required in non-worker mode).")
)

// TODO(herohde) 5/16/2017: if we were to move the dispatch to the beam package,
// it would imply that it had a flag (runner) -- and we'd need to change the
// signature of execute to not use beam to include local as default.
//
// We should probably also add an indirection for Init, too, so that we can
// move the worker flags to harness (or wrapper). Then dataflow could register
// both aspects when imported. It would no longer be available by default.

// Init is the hook that all user code must call, for now.
func Init() {
	if !*worker {
		return
	}

	// Since Init() is hijacking main, it's appropriate to do as main
	// does, and establish the background context here.
	ctx := context.Background()
	if err := harness.Main(ctx, *loggingEndpoint, *controlEndpoint); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}

	log.Print("Worker exited successfully!")
	for {
		// TODO: Flush logs? For now, just hang around until we're terminated.
		time.Sleep(time.Hour)
	}
}

var runners = map[string]func(context.Context, *beam.Pipeline) error{
	"local":    local.Execute,
	"dataflow": dataflow.Execute,
	"dot":      dot.Execute,
}

// Register associates the name with the supplied runner, making it available
// to execute a pipeline.
func Register(name string, fn func(context.Context, *beam.Pipeline) error) {
	if _, ok := runners[name]; ok {
		panic(fmt.Sprintf("runner %v already defined", name))
	}
	runners[name] = fn
}

// Run is a simple runner selector. Runners distributed with beam are pre-registered.
func Run(ctx context.Context, p *beam.Pipeline) error {
	if *worker {
		return fmt.Errorf("invalid call: failed to call Init at program startup")
	}

	fn, ok := runners[*runner]
	if !ok {
		return fmt.Errorf("runner not found: %v", *runner)
	}
	return fn(ctx, p)
}
