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

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
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

	// gRPC requires all writers to a stream be the same goroutine, so this is the
	// goroutine for managing responses back to the control service.
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
		plans:  make(map[string]*exec.Plan),
		active: make(map[string]*exec.Plan),
		data:   &DataManager{},
	}

	var cpuProfBuf bytes.Buffer

	// gRPC requires all readers of a stream be the same goroutine, so this goroutine
	// is responsible for managing the network data. All it does is pull data from
	// the stream, and hand off the message to a goroutine to actually be handled,
	// so as to avoid blocking the underlying network channel.
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

		// Launch a goroutine to handle the control message.
		// TODO(wcn): implement a rate limiter for 'heavy' messages?
		fn := func() {
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

		if req.GetProcessBundle() != nil {
			// Only process bundles in a goroutine. We at least need to process instructions for
			// each plan serially. Perhaps just invoke plan.Execute async?
			go fn()
		} else {
			fn()
		}
	}
}

type control struct {
	// plans that are candidates for execution.
	plans map[string]*exec.Plan // protected by mu
	// plans that are actively being executed.
	// a plan can only be in one of these maps at any time.
	active map[string]*exec.Plan // protected by mu
	mu     sync.Mutex

	data *DataManager
}

func (c *control) handleInstruction(ctx context.Context, req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
	id := req.GetInstructionId()
	ctx = context.WithValue(ctx, instKey, id)

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			p, err := exec.UnmarshalPlan(desc)
			if err != nil {
				return fail(id, "Invalid bundle desc: %v", err)
			}

			pid := desc.GetId()
			log.Debugf(ctx, "Plan %v: %v", pid, p)

			c.mu.Lock()
			c.plans[pid] = p
			c.mu.Unlock()
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
		c.mu.Lock()
		plan, ok := c.plans[ref]
		// Make the plan active, and remove it from candidates
		// since a plan can't be run concurrently.
		c.active[id] = plan
		delete(c.plans, ref)
		c.mu.Unlock()

		if !ok {
			return fail(id, "execution plan for %v not found", ref)
		}

		err := plan.Execute(ctx, id, c.data)

		// Move the plan back to the candidate state
		c.mu.Lock()
		c.plans[plan.ID()] = plan
		delete(c.active, id)
		c.mu.Unlock()

		if err != nil {
			return fail(id, "execute failed: %v", err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleResponse{},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		// log.Debugf(ctx, "PB Progress: %v", msg)

		ref := msg.GetInstructionReference()
		c.mu.Lock()
		plan, ok := c.active[ref]
		c.mu.Unlock()
		if !ok {
			return fail(id, "execution plan for %v not found", ref)
		}

		snapshot := plan.ProgressReport()

		return &fnpb.InstructionResponse{
			InstructionId: id,
			Response: &fnpb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{
					Metrics: &fnpb.Metrics{
						Ptransforms: map[string]*fnpb.Metrics_PTransform{
							snapshot.ID: &fnpb.Metrics_PTransform{
								ProcessedElements: &fnpb.Metrics_PTransform_ProcessedElements{
									Measured: &fnpb.Metrics_PTransform_Measured{
										OutputElementCounts: map[string]int64{
											snapshot.Name: snapshot.Count,
										},
									},
								},
							},
						},
					},
				},
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
	return grpcx.Dial(ctx, endpoint, timeout)
}
