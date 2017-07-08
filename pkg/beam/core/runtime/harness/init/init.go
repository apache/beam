// Package init contains the harness initialization code defined by the FnAPI.
// It is a separate package to avoid flags in the harness package and must be
// imported by the runner to ensure the init hook is registered.
package init

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
)

var (
	// The below 4 flags implement the Fn API container contract. Subject to change.
	worker          = flag.Bool("worker", false, "Whether binary is running in worker mode.")
	loggingEndpoint = flag.String("logging_endpoint", "", "Local logging gRPC endpoint (required in worker mode).")
	controlEndpoint = flag.String("control_endpoint", "", "Local control gRPC endpoint (required in worker mode).")
	persistDir      = flag.String("persist_dir", "", "Local semi-persistent directory (required in worker mode).")
	options         = flag.String("options", "", "JSON-encoded pipeline options (required in worker mode).")
)

func init() {
	runtime.RegisterInit(hook)
}

// TODO(herohde) 7/7/2017: Dataflow has a concept of sdk pipeline options and
// various values in this map:
//
// { "display_data": [...],
//   "options":{
//      "autoscalingAlgorithm":"NONE",
//      "dataflowJobId":"2017-07-07_xxx",
//      "gcpTempLocation":"",
//      "maxNumWorkers":0,
//      "numWorkers":1,
//      "project":"xxx",
//      "options": <Go SDK pipeline options>,
//  }}
//
// Which we may or may not want to be visible as first-class pipeline options.
// It is also TBD how/if to support global display data, but we certainly don't
// want it served back to the harness.

type wrapper struct {
	Options runtime.RawOptions `json:"options"`
}

// hook starts the harness, if in worker mode. Otherwise, is is a no-op.
func hook() {
	if !*worker {
		return
	}

	// TODO(herohde) 7/7/2017: options should be acquired through the FnAPI
	// instead, for example as part of a "login" message response.

	if *options != "" {
		var opt wrapper
		if err := json.Unmarshal([]byte(*options), &opt); err != nil {
			log.Fatalf("Failed to parse pipeline options: %v", err)
		}
		runtime.GlobalOptions.Import(opt.Options)
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
