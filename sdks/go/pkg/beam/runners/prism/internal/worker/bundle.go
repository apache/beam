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
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"golang.org/x/exp/slog"
)

// B represents an extant ProcessBundle instruction sent to an SDK worker.
// Generally manipulated by another package to interact with a worker.
type B struct {
	InstID string // ID for the instruction processing this bundle.
	PBDID  string // ID for the ProcessBundleDescriptor

	// InputTransformID is data being sent to the SDK.
	InputTransformID string
	InputData        [][]byte // Data specifically for this bundle.

	// TODO change to a single map[tid] -> map[input] -> map[window] -> struct { Iter data, MultiMap data } instead of all maps.
	// IterableSideInputData is a map from transformID, to inputID, to window, to data.
	IterableSideInputData map[string]map[string]map[typex.Window][][]byte
	// MultiMapSideInputData is a map from transformID, to inputID, to window, to data key, to data values.
	MultiMapSideInputData map[string]map[string]map[typex.Window]map[string][][]byte

	// OutputCount is the number of data outputs this bundle has.
	// We need to see this many closed data channels before the bundle is complete.
	OutputCount int
	// dataWait is how we determine if a bundle is finished, by waiting for each of
	// a Bundle's DataSinks to produce their last output.
	// After this point we can "commit" the bundle's output for downstream use.
	dataWait   sync.WaitGroup
	OutputData engine.TentativeData
	Resp       chan *fnpb.ProcessBundleResponse

	SinkToPCollection map[string]string

	// TODO: Metrics for this bundle, can be handled after the fact.
}

// Init initializes the bundle's internal state for waiting on all
// data and for relaying a response back.
func (b *B) Init() {
	// We need to see final data signals that match the number of
	// outputs the stage this bundle executes posesses
	b.dataWait.Add(b.OutputCount)
	b.Resp = make(chan *fnpb.ProcessBundleResponse, 1)
}

func (b *B) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("ID", b.InstID),
		slog.String("stage", b.PBDID))
}

// ProcessOn executes the given bundle on the given W, blocking
// until all data is complete.
//
// Assumes the bundle is initialized (all maps are non-nil, and data waitgroup is set, response channel initialized)
// Assumes the bundle descriptor is already registered with the W.
//
// While this method mostly manipulates a W, putting it on a B avoids mixing the workers
// public GRPC APIs up with local calls.
func (b *B) ProcessOn(wk *W) {
	wk.mu.Lock()
	wk.bundles[b.InstID] = b
	wk.mu.Unlock()

	slog.Debug("processing", "bundle", b, "worker", wk)

	// Tell the SDK to start processing the bundle.
	wk.InstReqs <- &fnpb.InstructionRequest{
		InstructionId: b.InstID,
		Request: &fnpb.InstructionRequest_ProcessBundle{
			ProcessBundle: &fnpb.ProcessBundleRequest{
				ProcessBundleDescriptorId: b.PBDID,
			},
		},
	}

	// TODO: make batching decisions.
	for i, d := range b.InputData {
		wk.DataReqs <- &fnpb.Elements{
			Data: []*fnpb.Elements_Data{
				{
					InstructionId: b.InstID,
					TransformId:   b.InputTransformID,
					Data:          d,
					IsLast:        i+1 == len(b.InputData),
				},
			},
		}
	}

	slog.Debug("waiting on data", "bundle", b)
	b.dataWait.Wait() // Wait until data is ready.
}
