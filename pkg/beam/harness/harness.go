package harness

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"google.golang.org/grpc"
	"log"
	"time"
)

// TODO(herohde) 2/8/2017: for now, assume we stage a full binary (not a plugin).

// Main is the main entrypoint for the Go harness. It runs at "runtime" -- not
// "pipeline-construction time" -- on each worker. It is a Fn API client and
// ultimately responsible for correctly executing user code.
func Main(ctx context.Context, endpoint string) error {
	log.Printf("Connecting via grpc @ %s ...", endpoint)

	conn, err := grpc.Dial(endpoint)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// (1) Redirect logging to logging api.

	if client, err := pb.NewBeamFnLoggingClient(conn).Logging(ctx); err == nil {
		defer client.CloseSend()

		now, _ := ptypes.TimestampProto(time.Now())

		list := &pb.LogEntry_List{
			LogEntries: []*pb.LogEntry{
				{
					Message:   "Gophers are majestic creatures!",
					Timestamp: now,
					Severity:  pb.LogEntry_CRITICAL,
				},
			},
		}
		if err := client.Send(list); err != nil {
			log.Printf("Didn't send: %v", err)
		}

		// TODO: actually setup redirection, similarly to the fluentd logger
	} else {
		return fmt.Errorf("Failed to connect to logging service: %v", err)
	}

	// (2) Connect to FnAPI control server. Receive and execute work.

	// TODO: setup data manager, DoFn register

	client, err := pb.NewBeamFnControlClient(conn).Control(ctx)
	if err != nil {
		return fmt.Errorf("Failed to connect to logging service: %v", err)
	}
	defer client.CloseSend()

	for {
		req, err := client.Recv()
		if err != nil {
			return fmt.Errorf("Recv failed: %v", err)
		}

		log.Printf("RECV: %v", req)

		// id := req.GetInstructionId()
		switch {
		case req.GetRegister() != nil:
			msg := req.GetRegister()

			for _, desc := range msg.GetProcessBundleDescriptor() {
				log.Printf("Got: %v", desc)
			}

		case req.GetProcessBundle() != nil:
			msg := req.GetProcessBundle()

			log.Printf("PB: %v", msg)

		case req.GetInitialSourceSplit() != nil:
			msg := req.GetInitialSourceSplit()

			log.Printf("ISS: %v", msg)

		case req.GetSourceProgress() != nil:
			msg := req.GetSourceProgress()

			log.Printf("SP: %v", msg)

		case req.GetDynamicSourceSplit() != nil:
			msg := req.GetDynamicSourceSplit()

			log.Printf("DSS: %v", msg)
		default:
			return fmt.Errorf("Unexpected request: %v", req)
		}

		// client.Send()
	}
}
