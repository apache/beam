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
	"sync/atomic"
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

	client := fnpb.NewBeamFnControlClient(conn)

	lookupDesc := func(id bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error) {
		pbd, err := client.GetProcessBundleDescriptor(ctx, &fnpb.GetProcessBundleDescriptorRequest{ProcessBundleDescriptorId: string(id)})
		log.Debugf(ctx, "GPBD RESP [%v]: %v, err %v", id, pbd, err)
		return pbd, err
	}

	stub, err := client.Control(ctx)
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

			if err := stub.Send(resp); err != nil {
				log.Errorf(ctx, "control.Send: Failed to respond: %v", err)
			}
		}
		log.Debugf(ctx, "control response channel closed")
	}()

	ctrl := &control{
		lookupDesc:  lookupDesc,
		descriptors: make(map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor),
		plans:       make(map[bundleDescriptorID][]*exec.Plan),
		active:      make(map[instructionID]*exec.Plan),
		failed:      make(map[instructionID]error),
		data:        &DataChannelManager{},
		state:       &StateChannelManager{},
	}

	// gRPC requires all readers of a stream be the same goroutine, so this goroutine
	// is responsible for managing the network data. All it does is pull data from
	// the stream, and hand off the message to a goroutine to actually be handled,
	// so as to avoid blocking the underlying network channel.
	var shutdown int32
	for {
		req, err := stub.Recv()
		if err != nil {
			// An error means we can't send or receive anymore. Shut down.
			atomic.AddInt32(&shutdown, 1)
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
			if resp != nil && atomic.LoadInt32(&shutdown) == 0 {
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
	lookupDesc  func(bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error)
	descriptors map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor // protected by mu
	// plans that are candidates for execution.
	plans map[bundleDescriptorID][]*exec.Plan // protected by mu
	// plans that are actively being executed.
	// a plan can only be in one of these maps at any time.
	active map[instructionID]*exec.Plan // protected by mu
	// plans that have failed during execution
	failed map[instructionID]error // protected by mu
	mu     sync.Mutex

	data  *DataChannelManager
	state *StateChannelManager
}

func (c *control) getOrCreatePlan(bdID bundleDescriptorID) (*exec.Plan, error) {
	c.mu.Lock()
	plans, ok := c.plans[bdID]
	var plan *exec.Plan
	if ok && len(plans) > 0 {
		plan = plans[len(plans)-1]
		c.plans[bdID] = plans[:len(plans)-1]
	} else {
		desc, ok := c.descriptors[bdID]
		if !ok {
			c.mu.Unlock() // Unlock to make the lookup.
			newDesc, err := c.lookupDesc(bdID)
			if err != nil {
				return nil, errors.WithContextf(err, "execution plan for %v not found", bdID)
			}
			c.mu.Lock()
			c.descriptors[bdID] = newDesc
			desc = newDesc
		}
		newPlan, err := exec.UnmarshalPlan(desc)
		if err != nil {
			c.mu.Unlock()
			return nil, errors.WithContextf(err, "invalid bundle desc: %v", bdID)
		}
		plan = newPlan
	}
	c.mu.Unlock()
	return plan, nil
}

func (c *control) handleInstruction(ctx context.Context, req *fnpb.InstructionRequest) *fnpb.InstructionResponse {
	instID := instructionID(req.GetInstructionId())
	ctx = setInstID(ctx, instID)

	switch {
	case req.GetRegister() != nil:
		msg := req.GetRegister()

		c.mu.Lock()
		for _, desc := range msg.GetProcessBundleDescriptor() {
			c.descriptors[bundleDescriptorID(desc.GetId())] = desc
		}
		c.mu.Unlock()

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
		plan, err := c.getOrCreatePlan(bdID)

		// Make the plan active.
		c.mu.Lock()
		c.active[instID] = plan
		c.mu.Unlock()

		if err != nil {
			return fail(ctx, instID, "Failed: %v", err)
		}

		data := NewScopedDataManager(c.data, instID)
		state := NewScopedStateReader(c.state, instID)
		err = plan.Execute(ctx, string(instID), exec.DataContext{Data: data, State: state})
		data.Close()
		state.Close()

		mons, pylds := monitoring(plan)
		// Move the plan back to the candidate state
		c.mu.Lock()
		// Mark the instruction as failed.
		if err != nil {
			c.failed[instID] = err
		}
		c.plans[bdID] = append(c.plans[bdID], plan)
		delete(c.active, instID)
		c.mu.Unlock()

		if err != nil {
			return fail(ctx, instID, "process bundle failed for instruction %v using plan %v : %v", instID, bdID, err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleResponse{
					MonitoringData:  pylds,
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

		mons, pylds := monitoring(plan)

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{
					MonitoringData:  pylds,
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
		sr, err := plan.Split(exec.SplitPoints{
			Splits:  ds.GetAllowedSplitPoints(),
			Frac:    ds.GetFractionOfRemainder(),
			BufSize: ds.GetEstimatedInputElements(),
		})

		if err != nil {
			return fail(ctx, instID, "unable to split %v: %v", ref, err)
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{
					PrimaryRoots: []*fnpb.BundleApplication{{
						TransformId: sr.TId,
						InputId:     sr.InId,
						Element:     sr.PS,
					}},
					ResidualRoots: []*fnpb.DelayedBundleApplication{{
						Application: &fnpb.BundleApplication{
							TransformId: sr.TId,
							InputId:     sr.InId,
							Element:     sr.RS,
						},
					}},
					ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{{
						LastPrimaryElement:   sr.PI,
						FirstResidualElement: sr.RI,
					}},
				},
			},
		}
	case req.GetProcessBundleProgressMetadata() != nil:
		msg := req.GetProcessBundleProgressMetadata()
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleProgressMetadata{
				ProcessBundleProgressMetadata: &fnpb.ProcessBundleProgressMetadataResponse{
					MonitoringInfo: shortIdsToInfos(msg.GetMonitoringInfoId()),
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
