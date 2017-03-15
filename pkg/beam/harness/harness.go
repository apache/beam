package harness

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	pb "github.com/apache/beam/sdks/go/third_party/beam/org_apache_beam_fn_v1"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
	"time"
)

// TODO(herohde) 2/8/2017: for now, assume we stage a full binary (not a plugin).

// Main is the main entrypoint for the Go harness. It runs at "runtime" -- not
// "pipeline-construction time" -- on each worker. It is a Fn API client and
// ultimately responsible for correctly executing user code.
func Main(ctx context.Context, loggingEndpoint, controlEndpoint string) error {
	// TODO: we need a way to tell the harness that we (re)started and it should
	// assume we've seen no messages. Sleep, for now, to dodge startup race.
	time.Sleep(1 * time.Minute)

	setupRemoteLogging(ctx, loggingEndpoint)

	// Connect to FnAPI control server. Receive and execute work.
	// TODO: setup data manager, DoFn register

	go func() {
		for t := range time.Tick(10 * time.Second) {
			log.Printf("Tick: %v", t)
		}
	}()

	conn, err := connect(controlEndpoint, 20)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client, err := pb.NewBeamFnControlClient(conn).Control(ctx)
	if err != nil {
		return fmt.Errorf("Failed to connect to control service: %v", err)
	}

	log.Printf("Successfully connected to control @ %v", controlEndpoint)

	// Each ProcessBundle is a sub-graph of the original one.

	var wg sync.WaitGroup
	respc := make(chan *pb.InstructionResponse, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for resp := range respc {
			log.Printf("RESP: %v", proto.MarshalTextString(resp))

			if err := client.Send(resp); err != nil {
				log.Printf("Failed to respond: %v", err)
			}
		}
	}()

	ctrl := &control{make(map[string]*graph.Graph)}
	for {
		req, err := client.Recv()
		if err != nil {
			close(respc)
			wg.Wait()

			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("Recv failed: %v", err)
		}

		log.Printf("RECV: %v", proto.MarshalTextString(req))

		resp := ctrl.handleInstruction(ctx, req)
		if resp != nil {
			respc <- resp
		}
	}
}

type control struct {
	graphs map[string]*graph.Graph

	// TODO: running pipelines
}

func (c *control) handleInstruction(ctx context.Context, req *pb.InstructionRequest) *pb.InstructionResponse {
	id := req.GetInstructionId()
	dummy := &pb.InstructionResponse_Register{Register: &pb.RegisterResponse{}}

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			g, err := translateBundle(desc)
			if err != nil {
				return &pb.InstructionResponse{
					InstructionId: id,
					Error:         fmt.Sprintf("Invalid bundle %v: %v", desc, err),
					Response:      dummy,
				}
			}
			c.graphs[desc.GetId()] = g
			log.Printf("Added subgraph %v:\n %v", desc.GetId(), g)
		}

		return &pb.InstructionResponse{
			InstructionId: id,
			Response: &pb.InstructionResponse_Register{
				Register: &pb.RegisterResponse{},
			},
		}

	case req.GetProcessBundle() != nil:
		msg := req.GetProcessBundle()

		// NOTE: the harness sends a 0-length process bundle request to sources (changed?)

		log.Printf("PB: %v", msg)

		ref := msg.GetProcessBundleDescriptorReference()
		g, ok := c.graphs[ref]
		if !ok {
			return &pb.InstructionResponse{
				InstructionId: id,
				Error:         fmt.Sprintf("Subgraph %v not found", ref),
				Response:      dummy,
			}
		}

		if err := local.ExecuteBundle(ctx, g.FakeBuild()); err != nil {
			return &pb.InstructionResponse{
				InstructionId: id,
				Error:         fmt.Sprintf("Execute failed: %v", err),
				Response:      dummy,
			}
		}

		return &pb.InstructionResponse{
			InstructionId: id,
			Response: &pb.InstructionResponse_ProcessBundle{
				ProcessBundle: &pb.ProcessBundleResponse{},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		log.Printf("PB Progress: %v", msg)

		return nil
		return &pb.InstructionResponse{
			InstructionId: id,
			Response: &pb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &pb.ProcessBundleProgressResponse{},
			},
		}

	case req.GetProcessBundleSplit() != nil:
		msg := req.GetProcessBundleSplit()

		log.Printf("PB Split: %v", msg)

		return nil
		return &pb.InstructionResponse{
			InstructionId: id,
			Response: &pb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &pb.ProcessBundleSplitResponse{},
			},
		}

	default:
		return &pb.InstructionResponse{
			InstructionId: req.GetInstructionId(),
			Error:         fmt.Sprintf("Unexpected request: %v", req),
			Response:      dummy,
		}
	}
}

func connect(endpoint string, attempts int) (*grpc.ClientConn, error) {
	log.Printf("Connecting via grpc @ %s ...", endpoint)

	for i := 0; i < attempts; i++ {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
		if err == nil {
			return conn, nil
		}

		log.Printf("Failed to connect to %s: %v", endpoint, err)
		time.Sleep(5 * time.Second)
	}
	return nil, fmt.Errorf("failed to connect to %s in %v attempts", endpoint, attempts)
}
