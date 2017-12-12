// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/golang/protobuf/proto"
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

	client, err := fnpb.NewBeamFnControlClient(conn).Control(ctx)
	if err != nil {
		return fmt.Errorf("Failed to connect to control service: %v", err)
	}

	log.Debugf(ctx, "Successfully connected to control @ %v", controlEndpoint)

	// Each ProcessBundle is a sub-graph of the original one.

	var wg sync.WaitGroup
	respc := make(chan *fnpb.InstructionResponse, 100)

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
			return fmt.Errorf("recv failed: %v", err)
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

func (c *control) handleInstruction(ctx context.Context, req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
	id := req.GetInstructionId()
	ctx = context.WithValue(ctx, instKey, id)

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			var roots []string
			for id, _ := range desc.GetTransforms() {
				roots = append(roots, id)
			}
			p := &pb.Pipeline{
				Components: &pb.Components{
					Transforms:          desc.Transforms,
					Pcollections:        desc.Pcollections,
					Coders:              desc.Coders,
					WindowingStrategies: desc.WindowingStrategies,
					Environments:        desc.Environments,
				},
				RootTransformIds: roots,
			}

			g, err := graphx.Unmarshal(p)
			if err != nil {
				return fail(id, "Invalid bundle desc: %v", err)
			}
			c.graphs[desc.GetId()] = g
			log.Debugf(ctx, "Added subgraph %v:\n %v", desc.GetId(), g)
		}

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_Register{
				Register: &fnpb.RegisterResponse{},
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

		if err := direct.ExecuteInternal(ctx, c.data, id, edges); err != nil {
			return fail(id, "Execute failed: %v", err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleResponse{},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		log.Debugf(ctx, "PB Progress: %v", msg)

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{},
			},
		}

	case req.GetProcessBundleSplit() != nil:
		msg := req.GetProcessBundleSplit()

		log.Debugf(ctx, "PB Split: %v", msg)

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{},
			},
		}

	default:
		return fail(id, "Unexpected request: %v", req)
	}
}

func fail(id, format string, args ...interface{}) *fnpb.InstructionResponse {
	dummy := &fnpb.InstructionResponse_Register{Register: &fnpb.RegisterResponse{}}

	return &fnpb.InstructionResponse{
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
