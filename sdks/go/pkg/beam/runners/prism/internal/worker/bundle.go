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

package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
)

// SideInputKey is for data lookups for a given bundle.
type SideInputKey struct {
	TransformID, Local string
}

// B represents an extant ProcessBundle instruction sent to an SDK worker.
// Generally manipulated by another package to interact with a worker.
type B struct {
	InstID string // ID for the instruction processing this bundle.
	PBDID  string // ID for the ProcessBundleDescriptor

	// InputTransformID is where data is being sent to in the SDK.
	InputTransformID       string
	Input                  []*engine.Block // Data and Timers for this bundle.
	EstimatedInputElements int
	HasTimers              []engine.StaticTimerID // Timer streams to terminate.

	// IterableSideInputData is a map from transformID + inputID, to window, to data.
	IterableSideInputData map[SideInputKey]map[typex.Window][][]byte
	// MultiMapSideInputData is a map from transformID + inputID, to window, to data key, to data values.
	MultiMapSideInputData map[SideInputKey]map[typex.Window]map[string][][]byte

	// State lives in OutputData

	// OutputCount is the number of data or timer outputs this bundle has.
	// We need to see this many closed data channels before the bundle is complete.
	OutputCount int
	// DataWait is how we determine if a bundle is finished, by waiting for each of
	// a Bundle's DataSinks to produce their last output.
	// After this point we can "commit" the bundle's output for downstream use.
	DataWait   chan struct{}
	dataSema   atomic.Int32
	OutputData engine.TentativeData

	Resp      chan *fnpb.ProcessBundleResponse
	BundleErr error
	responded bool

	SinkToPCollection map[string]string
}

// Init initializes the bundle's internal state for waiting on all
// data and for relaying a response back.
func (b *B) Init() {
	// We need to see final data and timer signals that match the number of
	// outputs the stage this bundle executes posesses
	outCap := int32(b.OutputCount + len(b.HasTimers))
	b.dataSema.Store(outCap)
	b.DataWait = make(chan struct{})
	if outCap == 0 {
		close(b.DataWait) // Can happen if there are no outputs for the bundle.
	}
	b.Resp = make(chan *fnpb.ProcessBundleResponse, 1)
}

// DataOrTimerDone indicates a final element has been received from a Data or Timer output.
func (b *B) DataOrTimerDone() {
	sema := b.dataSema.Add(-1)
	if sema == 0 {
		close(b.DataWait)
	}
}

func (b *B) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("ID", b.InstID),
		slog.String("stage", b.PBDID))
}

func (b *B) Respond(resp *fnpb.InstructionResponse) {
	if b.responded {
		slog.Warn("additional bundle response", "bundle", b, "resp", resp)
		return
	}
	b.responded = true
	if resp.GetError() != "" {
		b.BundleErr = fmt.Errorf("bundle %v %v failed:%v", resp.GetInstructionId(), b.PBDID, resp.GetError())
		close(b.Resp)
		return
	}
	b.Resp <- resp.GetProcessBundle()
}

// ProcessOn executes the given bundle on the given W.
// The returned channel is closed once all expected data is returned.
//
// Assumes the bundle is initialized (all maps are non-nil, and data waitgroup is set, response channel initialized)
// Assumes the bundle descriptor is already registered with the W.
//
// While this method mostly manipulates a W, putting it on a B avoids mixing the workers
// public GRPC APIs up with local calls.
func (b *B) ProcessOn(ctx context.Context, wk *W) <-chan struct{} {
	wk.mu.Lock()
	wk.activeInstructions[b.InstID] = b
	wk.mu.Unlock()

	slog.Debug("processing", "bundle", b, "worker", wk)

	// Tell the SDK to start processing the bundle.
	req := &fnpb.InstructionRequest{
		InstructionId: b.InstID,
		Request: &fnpb.InstructionRequest_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleRequest{
				ProcessBundleDescriptorId: b.PBDID,
			},
		},
	}
	select {
	case <-wk.StoppedChan:
		// The worker was stopped before req was sent.
		// Quit to avoid sending on a closed channel.
		outCap := b.OutputCount + len(b.HasTimers)
		for i := 0; i < outCap; i++ {
			b.DataOrTimerDone()
		}
		return b.DataWait
	case wk.InstReqs <- req:
		// desired outcome
	}

	// TODO: make batching decisions on the maxium to send per elements block, to reduce processing time overhead.
	for _, block := range b.Input {
		elms := &fnpb.Elements{}

		dataBuf := bytes.Join(block.Bytes, []byte{})
		switch block.Kind {
		case engine.BlockData:
			elms.Data = []*fnpb.Elements_Data{
				{
					InstructionId: b.InstID,
					TransformId:   b.InputTransformID,
					Data:          dataBuf,
				},
			}
		case engine.BlockTimer:
			elms.Timers = []*fnpb.Elements_Timers{
				{
					InstructionId: b.InstID,
					TransformId:   block.Transform,
					TimerFamilyId: block.Family,
					Timers:        dataBuf,
				},
			}
		default:
			panic("unknown engine.Block kind")
		}

		select {
		case <-wk.StoppedChan:
			b.DataOrTimerDone()
			return b.DataWait
		case <-ctx.Done():
			b.DataOrTimerDone()
			return b.DataWait
		case wk.DataReqs <- elms:
		}
	}

	// Send last of everything for now.
	timers := make([]*fnpb.Elements_Timers, 0, len(b.HasTimers))
	for _, tid := range b.HasTimers {
		timers = append(timers, &fnpb.Elements_Timers{
			InstructionId: b.InstID,
			TransformId:   tid.Transform,
			TimerFamilyId: tid.TimerFamily,
			IsLast:        true,
		})
	}
	select {
	case <-wk.StoppedChan:
		b.DataOrTimerDone()
		return b.DataWait
	case <-ctx.Done():
		b.DataOrTimerDone()
		return b.DataWait
	case wk.DataReqs <- &fnpb.Elements{
		Timers: timers,
		Data: []*fnpb.Elements_Data{
			{
				InstructionId: b.InstID,
				TransformId:   b.InputTransformID,
				IsLast:        true,
			},
		},
	}:
	}

	return b.DataWait
}

// Cleanup unregisters the bundle from the worker.
func (b *B) Cleanup(wk *W) {
	wk.mu.Lock()
	delete(wk.activeInstructions, b.InstID)
	wk.mu.Unlock()
}

func (b *B) Finalize(ctx context.Context, wk *W) (*fnpb.FinalizeBundleResponse, error) {
	resp := wk.sendInstruction(ctx, &fnpb.InstructionRequest{
		Request: &fnpb.InstructionRequest_FinalizeBundle{
			FinalizeBundle: &fnpb.FinalizeBundleRequest{
				InstructionId: b.InstID,
			},
		},
	})
	return resp.GetFinalizeBundle(), nil
}

// Progress sends a progress request for the given bundle to the passed in worker, blocking on the response.
func (b *B) Progress(ctx context.Context, wk *W) (*fnpb.ProcessBundleProgressResponse, error) {
	resp := wk.sendInstruction(ctx, &fnpb.InstructionRequest{
		Request: &fnpb.InstructionRequest_ProcessBundleProgress{
			ProcessBundleProgress: &fnpb.ProcessBundleProgressRequest{
				InstructionId: b.InstID,
			},
		},
	})
	if resp.GetError() != "" {
		return nil, fmt.Errorf("progress[%v] error from SDK: %v", b.InstID, resp.GetError())
	}
	return resp.GetProcessBundleProgress(), nil
}

// Split sends a split request for the given bundle to the passed in worker, blocking on the response.
func (b *B) Split(ctx context.Context, wk *W, fraction float64, allowedSplits []int64) (*fnpb.ProcessBundleSplitResponse, error) {
	resp := wk.sendInstruction(ctx, &fnpb.InstructionRequest{
		Request: &fnpb.InstructionRequest_ProcessBundleSplit{
			ProcessBundleSplit: &fnpb.ProcessBundleSplitRequest{
				InstructionId: b.InstID,
				DesiredSplits: map[string]*fnpb.ProcessBundleSplitRequest_DesiredSplit{
					b.InputTransformID: {
						FractionOfRemainder:    fraction,
						AllowedSplitPoints:     allowedSplits,
						EstimatedInputElements: int64(b.EstimatedInputElements),
					},
				},
			},
		},
	})
	if resp.GetError() != "" {
		return nil, fmt.Errorf("split[%v] error from SDK: %v", b.InstID, resp.GetError())
	}
	return resp.GetProcessBundleSplit(), nil
}
