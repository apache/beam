package beamexec

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"github.com/apache/beam/sdks/go/pkg/beam/runtime/harness"
	"log"
	"time"
)

var (
	worker          = flag.Bool("worker", false, "Whether binary is running in worker mode.")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	persistDir      = flag.String("persist_dir", "", "Local semi-persistent directory (required in worker mode).")

	runner = flag.String("runner", "local", "Pipeline runner (required in non-worker mode).")
)

// Init is the hook that all user code must call, for now.
func Init(ctx context.Context) {
	if !*worker {
		return
	}

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
}

func Register(name string, fn func(context.Context, *beam.Pipeline) error) {
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
