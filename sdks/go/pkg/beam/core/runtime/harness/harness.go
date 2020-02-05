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
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

// TODO(herohde) 2/8/2017: for now, assume we stage a full binary (not a plugin).

// Main is the main entrypoint for the Go harness. It runs at "runtime" -- not
// "pipeline-construction time" -- on each worker. It is a FnAPI client and
// ultimately responsible for correctly executing user code.
func Main(ctx context.Context, loggingEndpoint, controlEndpoint string) error {
	hooks.DeserializeHooksFromOptions(ctx)

	hooks.RunInitHooks(ctx)
	setupRemoteLogging(ctx, loggingEndpoint)
	recordHeader()

	// Connect to FnAPI control server. Receive and execute work.
	// TODO: setup data manager, DoFn register

	conn, err := dial(ctx, controlEndpoint, 60*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to connect")
	}
	defer conn.Close()

	client, err := fnpb.NewBeamFnControlClient(conn).Control(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to control service")
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
				log.Errorf(ctx, "control.Send: Failed to respond: %v", err)
			}
		}
	}()

	ctrl := &control{
		plans:  make(map[bundleDescriptorID]*exec.Plan),
		active: make(map[instructionID]*exec.Plan),
		failed: make(map[instructionID]error),
		data:   &DataChannelManager{},
		state:  &StateChannelManager{},
	}

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
			return errors.Wrapf(err, "control.Recv failed")
		}

		// Launch a goroutine to handle the control message.
		// TODO(wcn): implement a rate limiter for 'heavy' messages?
		fn := func(ctx context.Context, req *fnpb.InstructionRequest) {
			log.Debugf(ctx, "RECV: %v", proto.MarshalTextString(req))
			recordInstructionRequest(req)

			ctx = hooks.RunRequestHooks(ctx, req)
			resp := ctrl.handleInstruction(ctx, req)

			hooks.RunResponseHooks(ctx, req, resp)

			recordInstructionResponse(resp)
			if resp != nil {
				respc <- resp
			}
		}

		if req.GetProcessBundle() != nil {
			// Only process bundles in a goroutine. We at least need to process instructions for
			// each plan serially. Perhaps just invoke plan.Execute async?
			go fn(ctx, req)
		} else {
			fn(ctx, req)
		}
	}
}

type bundleDescriptorID string
type instructionID string

type control struct {
	// plans that are candidates for execution.
	plans map[bundleDescriptorID]*exec.Plan // protected by mu
	// plans that are actively being executed.
	// a plan can only be in one of these maps at any time.
	active map[instructionID]*exec.Plan // protected by mu
	// plans that have failed during execution
	failed map[instructionID]error // protected by mu
	mu     sync.Mutex

	data  *DataChannelManager
	state *StateChannelManager
}

func (c *control) handleInstruction(ctx context.Context, req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
	instID := instructionID(req.GetInstructionId())
	ctx = setInstID(ctx, instID)

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		for _, desc := range msg.GetProcessBundleDescriptor() {
			p, err := exec.UnmarshalPlan(desc)
			if err != nil {
				return fail(ctx, instID, "Invalid bundle desc: %v", err)
			}

			bdID := bundleDescriptorID(desc.GetId())
			log.Debugf(ctx, "Plan %v: %v", bdID, p)

			c.mu.Lock()
			c.plans[bdID] = p
			c.mu.Unlock()
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_Register{
				Register: &fnpb.RegisterResponse{},
			},
		}

	case req.GetProcessBundle() != nil:
		msg := req.GetProcessBundle()

		// NOTE: the harness sends a 0-length process bundle request to sources (changed?)

		bdID := bundleDescriptorID(msg.GetProcessBundleDescriptorId())
		log.Debugf(ctx, "PB [%v]: %v", instID, msg)
		c.mu.Lock()
		plan, ok := c.plans[bdID]
		// Make the plan active, and remove it from candidates
		// since a plan can't be run concurrently.
		c.active[instID] = plan
		delete(c.plans, bdID)
		c.mu.Unlock()

		if !ok {
			return fail(ctx, instID, "execution plan for %v not found", bdID)
		}

		data := NewScopedDataManager(c.data, instID)
		state := NewScopedStateReader(c.state, instID)
		err := plan.Execute(ctx, string(instID), exec.DataContext{Data: data, State: state})
		data.Close()
		state.Close()

		mets, mons := monitoring(plan)
		// Move the plan back to the candidate state
		c.mu.Lock()
		// Mark the instruction as failed.
		if err != nil {
			c.failed[instID] = err
		}
		c.plans[bdID] = plan
		delete(c.active, instID)
		c.mu.Unlock()

		if err != nil {
			return fail(ctx, instID, "process bundle failed for instruction %v using plan %v : %v", instID, bdID, err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleResponse{
					// TODO(lostluck): Delete legacy monitoring Metrics once they can be safely dropped.
					Metrics:         mets,
					MonitoringInfos: mons,
				},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		ref := instructionID(msg.GetInstructionId())
		c.mu.Lock()
		plan, ok := c.active[ref]
		err := c.failed[ref]
		c.mu.Unlock()
		if err != nil {
			return fail(ctx, instID, "failed to return progress: instruction %v failed: %v", ref, err)
		}
		if !ok {
			return fail(ctx, instID, "failed to return progress: instruction %v not active", ref)
		}

		mets, mons := monitoring(plan)

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{
					// TODO(lostluck): Delete legacy monitoring Metrics once they can be safely dropped.
					Metrics:         mets,
					MonitoringInfos: mons,
				},
			},
		}

	case req.GetProcessBundleSplit() != nil:
		msg := req.GetProcessBundleSplit()

		log.Debugf(ctx, "PB Split: %v", msg)
		ref := instructionID(msg.GetInstructionId())
		c.mu.Lock()
		plan, ok := c.active[ref]
		err := c.failed[ref]
		c.mu.Unlock()
		if err != nil {
			return fail(ctx, instID, "failed to split: instruction %v failed: %v", ref, err)
		}
		if !ok {
			return fail(ctx, instID, "failed to split: execution plan for %v not active", ref)
		}

		// Get the desired splits for the root FnAPI read operation.
		ds := msg.GetDesiredSplits()[plan.SourcePTransformID()]
		if ds == nil {
			return fail(ctx, instID, "failed to split: desired splits for root of %v was empty.", ref)
		}
		split, err := plan.Split(exec.SplitPoints{Splits: ds.GetAllowedSplitPoints(), Frac: ds.GetFractionOfRemainder()})

		if err != nil {
			return fail(ctx, instID, "unable to split %v: %v", ref, err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{
					ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{
						&fnpb.ProcessBundleSplitResponse_ChannelSplit{
							LastPrimaryElement:   split - 1,
							FirstResidualElement: split,
						},
					},
				},
			},
		}

	default:
		return fail(ctx, instID, "Unexpected request: %v", req)
	}
}

func fail(ctx context.Context, id instructionID, format string, args ...interface{}) *fnpb.InstructionResponse {
	log.Output(ctx, log.SevError, 1, fmt.Sprintf(format, args...))
	dummy := &fnpb.InstructionResponse_Register{Register: &fnpb.RegisterResponse{}}

	return &fnpb.InstructionResponse{
		InstructionId: string(id),
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
