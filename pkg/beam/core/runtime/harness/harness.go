// Package harness implements the SDK side of the Beam FnAPI.
package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/api/org_apache_beam_fn_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/local"
	"google.golang.org/grpc"
)

// TODO(herohde) 2/8/2017: for now, assume we stage a full binary (not a plugin).

// Main is the main entrypoint for the Go harness. It runs at "runtime" -- not
// "pipeline-construction time" -- on each worker. It is a Fn API client and
// ultimately responsible for correctly executing user code.
func Main(ctx context.Context, loggingEndpoint, controlEndpoint string) error {
	setupRemoteLogging(ctx, loggingEndpoint)
	setupDiagnosticRecording()

	recordHeader()

	// Connect to FnAPI control server. Receive and execute work.
	// TODO: setup data manager, DoFn register

	conn, err := dial(ctx, controlEndpoint, 60*time.Second)
	if err != nil {
		return fmt.Errorf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client, err := pb.NewBeamFnControlClient(conn).Control(ctx)
	if err != nil {
		return fmt.Errorf("Failed to connect to control service: %v", err)
	}

	log.Debugf(ctx, "Successfully connected to control @ %v", controlEndpoint)

	// Each ProcessBundle is a sub-graph of the original one.

	var wg sync.WaitGroup
	respc := make(chan *pb.InstructionResponse, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for resp := range respc {
			log.Debugf(ctx, "RESP: %v", proto.MarshalTextString(resp))

			if err := client.Send(resp); err != nil {
				log.Errorf(ctx, "Failed to respond: %v", err)
			}
		}
	}()

	ctrl := &control{
		graphs: make(map[string]*graph.Graph),
		data:   &DataManager{},
	}

	var cpuProfBuf bytes.Buffer
	for {
		req, err := client.Recv()
		if err != nil {
			close(respc)
			wg.Wait()

			if err == io.EOF {
				recordFooter()
				return nil
			}
			return fmt.Errorf("Recv failed: %v", err)
		}

		log.Debugf(ctx, "RECV: %v", proto.MarshalTextString(req))
		recordInstructionRequest(req)

		if isEnabled("cpu_profiling") {
			cpuProfBuf.Reset()
			pprof.StartCPUProfile(&cpuProfBuf)
		}
		resp := ctrl.handleInstruction(ctx, req)

		if isEnabled("cpu_profiling") {
			pprof.StopCPUProfile()
			if err := ioutil.WriteFile(fmt.Sprintf("%s/cpu_prof%s", storagePath, req.InstructionId), cpuProfBuf.Bytes(), 0644); err != nil {
				log.Warnf(ctx, "Failed to write CPU profile for instruction %s: %v", req.InstructionId, err)
			}
		}

		recordInstructionResponse(resp)
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
	ctx = context.WithValue(ctx, instKey, id)

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			g, err := translate(desc)
			if err != nil {
				return fail(id, "Invalid bundle desc: %v", err)
			}
			c.graphs[desc.GetId()] = g
			log.Debugf(ctx, "Added subgraph %v:\n %v", desc.GetId(), g)
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

		log.Debugf(ctx, "PB: %v", msg)

		ref := msg.GetProcessBundleDescriptorReference()
		g, ok := c.graphs[ref]
		if !ok {
			return fail(id, "Subgraph %v not found", ref)
		}

		// TODO: Async execution. The below assumes serial bundle execution.

		edges, _, err := g.Build()
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

		log.Debugf(ctx, "PB Progress: %v", msg)

		return &pb.InstructionResponse{
			InstructionId: id,
			Response: &pb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &pb.ProcessBundleProgressResponse{},
			},
		}

	case req.GetProcessBundleSplit() != nil:
		msg := req.GetProcessBundleSplit()

		log.Debugf(ctx, "PB Split: %v", msg)

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

// dial to the specified endpoint. if timeout <=0, call blocks until
// grpc.Dial succeeds.
func dial(ctx context.Context, endpoint string, timeout time.Duration) (*grpc.ClientConn, error) {
	log.Infof(ctx, "Connecting via grpc @ %s ...", endpoint)

	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}

	// TODO(wcn): Update this code to not use deprecated grpc.WithTimeout
	if timeout > 0 {
		opts = append(opts, grpc.WithTimeout(time.Duration(timeout)*time.Second))
	}
	conn, err := grpc.Dial(endpoint, opts...)
	if err == nil {
		return conn, nil
	}
	return nil, fmt.Errorf("failed to connect to %s after %v seconds", endpoint, timeout)
}
