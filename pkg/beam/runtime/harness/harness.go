package harness

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	pb "github.com/apache/beam/sdks/go/pkg/beam/fnapi/org_apache_beam_fn_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"google.golang.org/grpc"
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
		for t := range time.Tick(30 * time.Second) {
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

	ctrl := &control{
		graphs: make(map[string]*graph.Graph),
		data:   &DataManager{},
	}
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
	data   *DataManager

	// TODO: running pipelines
}

func (c *control) handleInstruction(ctx context.Context, req *pb.InstructionRequest) *pb.InstructionResponse {
	id := req.GetInstructionId()

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			g, err := translate(desc)
			if err != nil {
				return fail(id, "Invalid bundle desc: %v", err)
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
			return fail(id, "Subgraph %v not found", ref)
		}

		// TODO: Async execution. The below assumes serial bundle execution.

		edges, err := g.Build()
		if err != nil {
			return fail(id, "Build failed: %v", err)
		}

		if err := local.ExecuteInternal(ctx, c.data, id, edges); err != nil {
			return fail(id, "Execute failed: %v", err)
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
		return fail(id, "Unexpected request: %v", req)
	}
}

func fail(id, format string, args ...interface{}) *pb.InstructionResponse {
	dummy := &pb.InstructionResponse_Register{Register: &pb.RegisterResponse{}}

	return &pb.InstructionResponse{
		InstructionId: id,
		Error:         fmt.Sprintf(format, args...),
		Response:      dummy,
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
