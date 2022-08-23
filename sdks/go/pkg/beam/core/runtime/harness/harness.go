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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/profiler"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/statecache"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/diagnostics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// URNMonitoringInfoShortID is a URN indicating support for short monitoring info IDs.
const URNMonitoringInfoShortID = "beam:protocol:monitoring_info_short_ids:v1"

// TODO(herohde) 2/8/2017: for now, assume we stage a full binary (not a plugin).

// Main is the main entrypoint for the Go harness. It runs at "runtime" -- not
// "pipeline-construction time" -- on each worker. It is a FnAPI client and
// ultimately responsible for correctly executing user code.
func Main(ctx context.Context, loggingEndpoint, controlEndpoint string) error {
	hooks.DeserializeHooksFromOptions(ctx)

	// Extract environment variables. These are optional runner supported capabilities.
	// Expected env variables:
	// RUNNER_CAPABILITIES : list of runner supported capability urn.
	// STATUS_ENDPOINT : Endpoint to connect to status server used for worker status reporting.
	statusEndpoint := os.Getenv("STATUS_ENDPOINT")
	runnerCapabilities := strings.Split(os.Getenv("RUNNER_CAPABILITIES"), " ")
	rcMap := make(map[string]bool)
	if len(runnerCapabilities) > 0 {
		for _, capability := range runnerCapabilities {
			rcMap[capability] = true
		}
	}

	// Pass in the logging endpoint for use w/the default remote logging hook.
	ctx = context.WithValue(ctx, loggingEndpointCtxKey, loggingEndpoint)
	ctx, err := hooks.RunInitHooks(ctx)
	if err != nil {
		return err
	}

	// Check for environment variables for cloud profiling.
	// If both present, start running profiler.
	if name, id := os.Getenv("CLOUD_PROF_JOB_NAME"), os.Getenv("CLOUD_PROF_JOB_ID"); name != "" && id != "" {
		log.Debugf(ctx, "enabling cloud profiling for job name: %v, job id: %v", name, id)
		cfg := profiler.Config{
			Service:        name,
			ServiceVersion: id,
		}
		if err := profiler.Start(cfg); err != nil {
			log.Errorf(ctx, "failed to start cloud profiler, got %v", err)
		}
	}

	if tempLocation := beam.PipelineOptions.Get("temp_location"); tempLocation != "" && samplingFrequencySeconds > 0 {
		go diagnostics.SampleForHeapProfile(ctx, samplingFrequencySeconds, maxTimeBetweenDumpsSeconds)
	}

	recordHeader()

	// Connect to FnAPI control server. Receive and execute work.
	conn, err := dial(ctx, controlEndpoint, "control", 60*time.Second)
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

	sideCache := statecache.SideInputCache{}
	sideCache.Init(cacheSize)

	ctrl := &control{
		lookupDesc:           lookupDesc,
		descriptors:          make(map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor),
		plans:                make(map[bundleDescriptorID][]*exec.Plan),
		active:               make(map[instructionID]*exec.Plan),
		awaitingFinalization: make(map[instructionID]awaitingFinalization),
		inactive:             newCircleBuffer(),
		metStore:             make(map[instructionID]*metrics.Store),
		failed:               make(map[instructionID]error),
		data:                 &DataChannelManager{},
		state:                &StateChannelManager{},
		cache:                &sideCache,
		runnerCapabilities:   rcMap,
	}

	// if the runner supports worker status api then expose SDK harness status
	if statusEndpoint != "" {
		statusHandler, err := newWorkerStatusHandler(ctx, statusEndpoint, ctrl.cache, func(statusInfo *strings.Builder) { ctrl.metStoreToString(statusInfo) })
		if err != nil {
			log.Errorf(ctx, "error establishing connection to worker status API: %v", err)
		} else {
			if err := statusHandler.start(ctx); err == nil {
				defer statusHandler.stop(ctx)
			}
		}
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
			// Add this to the inactive queue before allowing other requests
			// to be processed. This prevents race conditions with split
			// or progress requests for this instruction.
			ctrl.mu.Lock()
			ctrl.inactive.Add(instructionID(req.GetInstructionId()))
			ctrl.mu.Unlock()
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

const circleBufferCap = 1000

// circleBuffer is an ordered eviction buffer
type circleBuffer struct {
	buf map[instructionID]struct{}
	// order that instructions should be removed from the buf map.
	// treated like a circular buffer with nextRemove as the pointer.
	removeQueue [circleBufferCap]instructionID
	nextRemove  int
}

func newCircleBuffer() circleBuffer {
	return circleBuffer{buf: map[instructionID]struct{}{}}
}

// Add the instruction to the buffer without including it in the remove queue.
func (c *circleBuffer) Add(instID instructionID) {
	c.buf[instID] = struct{}{}
}

// Remove deletes the value from the map.
func (c *circleBuffer) Remove(instID instructionID) {
	delete(c.buf, instID)
}

// Insert adds an instruction to the buffer, and removes one if necessary.
// If one is removed, it's returned so the instruction can be GCd from other
// maps.
func (c *circleBuffer) Insert(instID instructionID) (removed instructionID, ok bool) {
	// check if we need to evict something, and then do so.
	if len(c.buf) >= len(c.removeQueue) {
		removed = c.removeQueue[c.nextRemove]
		delete(c.buf, removed)
		ok = true
	}
	// nextRemove is now free, add the current instruction to the set.
	c.removeQueue[c.nextRemove] = instID
	c.buf[instID] = struct{}{}
	// increment and wrap around.
	c.nextRemove++
	if c.nextRemove >= len(c.removeQueue) {
		c.nextRemove = 0
	}
	return removed, ok
}

// Contains returns whether the buffer contains the given instruction.
func (c *circleBuffer) Contains(instID instructionID) bool {
	_, ok := c.buf[instID]
	return ok
}

type awaitingFinalization struct {
	expiration time.Time
	plan       *exec.Plan
	bdID       bundleDescriptorID
}

type control struct {
	lookupDesc  func(bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error)
	descriptors map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor // protected by mu
	// plans that are candidates for execution.
	plans map[bundleDescriptorID][]*exec.Plan // protected by mu
	// plans that are awaiting bundle finalization.
	awaitingFinalization map[instructionID]awaitingFinalization //protected by mu
	// plans that are actively being executed.
	// a plan can only be in one of these maps at any time.
	active map[instructionID]*exec.Plan // protected by mu
	// a plan that's either about to start or has finished recently
	// instructions in this queue should return empty responses to control messages.
	inactive circleBuffer // protected by mu
	// metric stores for active plans.
	metStore map[instructionID]*metrics.Store // protected by mu
	// plans that have failed during execution
	failed map[instructionID]error // protected by mu
	mu     sync.Mutex

	data  *DataChannelManager
	state *StateChannelManager
	// TODO(BEAM-11097): Cache is currently unused.
	cache              *statecache.SideInputCache
	runnerCapabilities map[string]bool
}

func (c *control) metStoreToString(statusInfo *strings.Builder) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for bundleID, store := range c.metStore {
		statusInfo.WriteString(fmt.Sprintf("Bundle ID: %v\n", bundleID))
		statusInfo.WriteString(fmt.Sprintf("\t%s", store.BundleState()))
		statusInfo.WriteString(fmt.Sprintf("\t%s", store.StateRegistry()))
	}
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
			return nil, errors.WithContextf(err, "invalid bundle desc: %v\n%v\n", bdID, desc.String())
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
		c.inactive.Remove(instID)
		c.active[instID] = plan
		// Get the user metrics store for this bundle.
		ctx = metrics.SetBundleID(ctx, string(instID))
		store := metrics.GetStore(ctx)
		c.metStore[instID] = store
		c.mu.Unlock()

		if err != nil {
			return fail(ctx, instID, "Failed: %v", err)
		}

		tokens := msg.GetCacheTokens()
		c.cache.SetValidTokens(tokens...)

		data := NewScopedDataManager(c.data, instID)
		state := NewScopedStateReaderWithCache(c.state, instID, c.cache)

		sampler := newSampler(store)
		go sampler.start(ctx, samplePeriod)

		err = plan.Execute(ctx, string(instID), exec.DataContext{Data: data, State: state})

		sampler.stop()

		data.Close()
		state.Close()

		c.cache.CompleteBundle(tokens...)

		mons, pylds := monitoring(plan, store, c.runnerCapabilities[URNMonitoringInfoShortID])

		requiresFinalization := false
		// Move the plan back to the candidate state
		c.mu.Lock()
		// Mark the instruction as failed.
		if err != nil {
			c.failed[instID] = err
		} else {
			// Non failure plans should either be moved to the finalized state
			// or to plans so they can be re-used.
			expiration := plan.GetExpirationTime()
			if time.Now().Before(expiration) {
				// TODO(BEAM-10976) - we can be a little smarter about data structures here by
				// by storing plans awaiting finalization in a heap. That way when we expire plans
				// here its O(1) instead of O(n) (though adding/finalizing will still be O(logn))
				requiresFinalization = true
				c.awaitingFinalization[instID] = awaitingFinalization{
					expiration: expiration,
					plan:       plan,
					bdID:       bdID,
				}
				// Move any plans that have exceeded their expiration back into the re-use pool
				for id, af := range c.awaitingFinalization {
					if time.Now().After(af.expiration) {
						c.plans[af.bdID] = append(c.plans[af.bdID], af.plan)
						delete(c.awaitingFinalization, id)
					}
				}
			} else {
				c.plans[bdID] = append(c.plans[bdID], plan)
			}
		}

		// Check if the underlying DoFn self-checkpointed.
		sr, delay, checkpointed, checkErr := plan.Checkpoint()

		var rRoots []*fnpb.DelayedBundleApplication
		if checkpointed {
			rRoots = make([]*fnpb.DelayedBundleApplication, len(sr.RS))
			for i, r := range sr.RS {
				rRoots[i] = &fnpb.DelayedBundleApplication{
					Application: &fnpb.BundleApplication{
						TransformId:      sr.TId,
						InputId:          sr.InId,
						Element:          r,
						OutputWatermarks: sr.OW,
					},
					RequestedTimeDelay: durationpb.New(delay),
				}
			}
		}

		delete(c.active, instID)
		if removed, ok := c.inactive.Insert(instID); ok {
			delete(c.failed, removed) // Also GC old failed bundles.
		}
		delete(c.metStore, instID)

		c.mu.Unlock()

		if err != nil {
			return fail(ctx, instID, "process bundle failed for instruction %v using plan %v : %v", instID, bdID, err)
		}

		if checkErr != nil {
			return fail(ctx, instID, "process bundle failed at checkpointing for instruction %v using plan %v : %v", instID, bdID, checkErr)
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: &fnpb.ProcessBundleResponse{
					ResidualRoots:        rRoots,
					MonitoringData:       pylds,
					MonitoringInfos:      mons,
					RequiresFinalization: requiresFinalization,
				},
			},
		}

	case req.GetFinalizeBundle() != nil:
		msg := req.GetFinalizeBundle()

		ref := instructionID(msg.GetInstructionId())

		af, ok := c.awaitingFinalization[ref]
		if !ok {
			return fail(ctx, instID, "finalize bundle failed for instruction %v: couldn't find plan in finalizing map", ref)
		}

		if time.Now().Before(af.expiration) {
			if err := af.plan.Finalize(); err != nil {
				return fail(ctx, instID, "finalize bundle failed for instruction %v using plan %v : %v", ref, af.bdID, err)
			}
		}
		c.plans[af.bdID] = append(c.plans[af.bdID], af.plan)
		delete(c.awaitingFinalization, ref)

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_FinalizeBundle{
				FinalizeBundle: &fnpb.FinalizeBundleResponse{},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		ref := instructionID(msg.GetInstructionId())

		plan, store, resp := c.getPlanOrResponse(ctx, "progress", instID, ref)
		if resp != nil {
			return resp
		}
		if plan == nil && resp == nil {
			return &fnpb.InstructionResponse{
				InstructionId: string(instID),
				Response: &fnpb.InstructionResponse_ProcessBundleProgress{
					ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{},
				},
			}
		}

		mons, pylds := monitoring(plan, store, c.runnerCapabilities[URNMonitoringInfoShortID])

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

		plan, _, resp := c.getPlanOrResponse(ctx, "split", instID, ref)
		if resp != nil {
			return resp
		}
		if plan == nil {
			return &fnpb.InstructionResponse{
				InstructionId: string(instID),
				Response: &fnpb.InstructionResponse_ProcessBundleSplit{
					ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{},
				},
			}
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

		var pRoots []*fnpb.BundleApplication
		var rRoots []*fnpb.DelayedBundleApplication
		if sr.PS != nil && len(sr.PS) > 0 && sr.RS != nil && len(sr.RS) > 0 {
			pRoots = make([]*fnpb.BundleApplication, len(sr.PS))
			for i, p := range sr.PS {
				pRoots[i] = &fnpb.BundleApplication{
					TransformId: sr.TId,
					InputId:     sr.InId,
					Element:     p,
				}
			}
			rRoots = make([]*fnpb.DelayedBundleApplication, len(sr.RS))
			for i, r := range sr.RS {
				rRoots[i] = &fnpb.DelayedBundleApplication{
					Application: &fnpb.BundleApplication{
						TransformId:      sr.TId,
						InputId:          sr.InId,
						Element:          r,
						OutputWatermarks: sr.OW,
					},
				}
			}
		}

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{
					ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{{
						TransformId:          plan.SourcePTransformID(),
						LastPrimaryElement:   sr.PI,
						FirstResidualElement: sr.RI,
					}},
					PrimaryRoots:  pRoots,
					ResidualRoots: rRoots,
				},
			},
		}
	case req.GetMonitoringInfos() != nil:
		msg := req.GetMonitoringInfos()
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_MonitoringInfos{
				MonitoringInfos: &fnpb.MonitoringInfosMetadataResponse{
					MonitoringInfo: shortIdsToInfos(msg.GetMonitoringInfoId()),
				},
			},
		}
	case req.GetHarnessMonitoringInfos() != nil:
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_HarnessMonitoringInfos{
				HarnessMonitoringInfos: &fnpb.HarnessMonitoringInfosResponse{
					// TODO(BEAM-11092): Populate with non-bundle metrics data.
					MonitoringData: map[string][]byte{},
				},
			},
		}

	default:
		return fail(ctx, instID, "Unexpected request: %v", req)
	}
}

// getPlanOrResponse returns the plan for the given instruction id.
// Otherwise, provides an error response.
// However, if that plan is known as inactive, it returns both the plan and response as nil,
// indicating that an empty response of the appropriate type must be returned instead.
// This is done because the OneOf types in Go protos are not exported, so we can't pass
// them as a parameter here instead, and relying on those proto internal would be brittle.
//
// Since this logic is subtle, it's been abstracted to a method to scope the defer unlock.
func (c *control) getPlanOrResponse(ctx context.Context, kind string, instID, ref instructionID) (*exec.Plan, *metrics.Store, *fnpb.InstructionResponse) {
	c.mu.Lock()
	plan, ok := c.active[ref]
	if !ok {
		var af awaitingFinalization
		af, ok = c.awaitingFinalization[ref]
		if ok {
			plan = af.plan
		}
	}
	err := c.failed[ref]
	store := c.metStore[ref]
	defer c.mu.Unlock()

	if err != nil {
		return nil, nil, fail(ctx, instID, "failed to return %v: instruction %v failed: %v", kind, ref, err)
	}
	if !ok {
		if c.inactive.Contains(ref) {
			return nil, nil, nil
		}
		return nil, nil, fail(ctx, instID, "failed to return %v: instruction %v not active", kind, ref)
	}
	return plan, store, nil
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
func dial(ctx context.Context, endpoint, purpose string, timeout time.Duration) (*grpc.ClientConn, error) {
	log.Infof(ctx, "Connecting via grpc @ %s for %s ...", endpoint, purpose)
	return grpcx.Dial(ctx, endpoint, timeout)
}
