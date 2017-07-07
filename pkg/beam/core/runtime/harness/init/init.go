// Package init contains the harness initialization code defined by the FnAPI.
// It is a separate package to avoid flags in the harness package and must be
// imported by the runner to ensure the init hook is registered.
package init

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
)

var (
	// The below 4 flags implement the Fn API container contract. Subject to change.
	worker          = flag.Bool("worker", false, "Whether binary is running in worker mode.")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	persistDir      = flag.String("persist_dir", "", "Local semi-persistent directory (required in worker mode).")
)

func init() {
	beam.RegisterInit(hook)
}

// hook starts the harness, if in worker mode. Otherwise, is is a no-op.
func hook() {
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
