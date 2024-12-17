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

package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/jobservices"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// link represents the tuple of a transform, the local id, and the global id for
// that transform's respective input or output. Which it is, is context dependant,
// and not knowable from just the link itself, but can be verified against the transform proto.
type link struct {
	Transform, Local, Global string
}

// stage represents a fused subgraph executed in a single environment.
//
// TODO: Consider ignoring environment boundaries and making fusion
// only consider necessary materialization breaks. The data protocol
// should in principle be able to connect two SDK environments directly
// instead of going through the runner at all, which would be a small
// efficiency gain, in runner memory use.
//
// That would also warrant an execution mode where fusion is taken into
// account, but all serialization boundaries remain since the pcollections
// would continue to get serialized.
type stage struct {
	ID                 string
	transforms         []string
	primaryInput       string          // PCollection used as the parallel input.
	outputs            []link          // PCollections that must escape this stage.
	sideInputs         []engine.LinkID // Non-parallel input PCollections and their consumers
	internalCols       []string        // PCollections that escape. Used for precise coder sending.
	envID              string
	finalize           bool
	stateful           bool
	onWindowExpiration engine.StaticTimerID

	// hasTimers indicates the transform+timerfamily pairs that need to be waited on for
	// the stage to be considered complete.
	hasTimers            []engine.StaticTimerID
	processingTimeTimers map[string]bool

	// stateTypeLen maps state values to encoded lengths for the type.
	// Only used for OrderedListState which must manipulate individual state datavalues.
	stateTypeLen map[engine.LinkID]func([]byte) int

	exe              transformExecuter
	inputTransformID string
	inputInfo        engine.PColInfo
	desc             *fnpb.ProcessBundleDescriptor
	prepareSides     func(b *worker.B, watermark mtime.Time)

	SinkToPCollection map[string]string
	OutputsToCoders   map[string]engine.PColInfo

	// Stage specific progress and splitting interval.
	baseProgTick atomic.Value // time.Duration
}

// The minimum and maximum durations between each ProgressBundleRequest and split evaluation.
const (
	minimumProgTick = 100 * time.Millisecond
	maximumProgTick = 30 * time.Second
)

func clampTick(dur time.Duration) time.Duration {
	switch {
	case dur < minimumProgTick:
		return minimumProgTick
	case dur > maximumProgTick:
		return maximumProgTick
	default:
		return dur
	}
}

func (s *stage) Execute(ctx context.Context, j *jobservices.Job, wk *worker.W, comps *pipepb.Components, em *engine.ElementManager, rb engine.RunBundle) (err error) {
	if s.baseProgTick.Load() == nil {
		s.baseProgTick.Store(minimumProgTick)
	}
	defer func() {
		// Convert execution panics to errors to fail the bundle.
		if e := recover(); e != nil {
			err = fmt.Errorf("panic in stage.Execute bundle processing goroutine: %v, stage: %+v,stackTrace:\n%s", e, s, debug.Stack())
		}
	}()
	slog.Debug("Execute: starting bundle", "bundle", rb)

	var b *worker.B
	initialState := em.StateForBundle(rb)
	var dataReady <-chan struct{}
	switch s.envID {
	case "": // Runner Transforms
		if len(s.transforms) != 1 {
			panic(fmt.Sprintf("unexpected number of runner transforms, want 1: %+v", s))
		}
		tid := s.transforms[0]
		// Runner transforms are processed immeadiately.
		b = s.exe.ExecuteTransform(s.ID, tid, comps.GetTransforms()[tid], comps, rb.Watermark, em.InputForBundle(rb, s.inputInfo))
		b.InstID = rb.BundleID
		slog.Debug("Execute: runner transform", "bundle", rb, slog.String("tid", tid))

		// Do some accounting for the fake bundle.
		b.Resp = make(chan *fnpb.ProcessBundleResponse, 1)
		close(b.Resp) // To avoid blocking downstream, since we don't send on this.
		closed := make(chan struct{})
		close(closed)
		dataReady = closed
	case wk.Env:
		input, estimatedElements := em.DataAndTimerInputForBundle(rb, s.inputInfo)
		b = &worker.B{
			PBDID:  s.ID,
			InstID: rb.BundleID,

			InputTransformID: s.inputTransformID,

			Input:                  input,
			EstimatedInputElements: estimatedElements,

			OutputData: initialState,
			HasTimers:  s.hasTimers,

			SinkToPCollection: s.SinkToPCollection,
			OutputCount:       len(s.outputs),
		}
		b.Init()

		s.prepareSides(b, rb.Watermark)

		slog.Debug("Execute: processing", "bundle", rb)
		defer b.Cleanup(wk)
		dataReady = b.ProcessOn(ctx, wk)
	default:
		err := fmt.Errorf("unknown environment[%v]", s.envID)
		slog.Error("Execute", "error", err)
		panic(err)
	}

	// Progress + split loop.
	previousIndex := int64(-2)
	previousTotalCount := int64(-2) // Total count of all pcollection elements.

	unsplit := true
	baseTick := s.baseProgTick.Load().(time.Duration)
	ticked := false
	progTick := time.NewTicker(baseTick)
	defer progTick.Stop()
	var dataFinished, bundleFinished bool
	// If we have no data outputs, we still need to have progress & splits
	// while waiting for bundle completion.
	if b.OutputCount == 0 {
		dataFinished = true
	}
	var resp *fnpb.ProcessBundleResponse
progress:
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case resp = <-b.Resp:
			bundleFinished = true
			if b.BundleErr != nil {
				return b.BundleErr
			}
			if dataFinished && bundleFinished {
				break progress // exit progress loop on close.
			}
		case <-dataReady:
			dataFinished = true
			if dataFinished && bundleFinished {
				break progress // exit progress loop on close.
			}
		case <-progTick.C:
			ticked = true
			resp, err := b.Progress(ctx, wk)
			if err != nil {
				slog.Debug("SDK Error from progress, aborting progress", "bundle", rb, "error", err.Error())
				break progress
			}
			index, unknownIDs := j.ContributeTentativeMetrics(resp)
			if len(unknownIDs) > 0 {
				md := wk.MonitoringMetadata(ctx, unknownIDs)
				j.AddMetricShortIDs(md)
			}
			slog.Debug("progress report", "bundle", rb, "index", index, "prevIndex", previousIndex)

			// Check if there has been any measurable progress by the input, or all output pcollections since last report.
			slow := previousIndex == index["index"] && previousTotalCount == index["totalCount"]
			if slow && unsplit {
				slog.Debug("splitting report", "bundle", rb, "index", index)
				sr, err := b.Split(ctx, wk, 0.5 /* fraction of remainder */, nil /* allowed splits */)
				if err != nil {
					slog.Warn("SDK Error from split, aborting splits", "bundle", rb, "error", err.Error())
					break progress
				}
				if sr.GetChannelSplits() == nil {
					slog.Debug("SDK returned no splits", "bundle", rb)
					unsplit = false
					continue progress
				}

				// TODO sort out rescheduling primary Roots on bundle failure.
				var residuals []engine.Residual
				for _, rr := range sr.GetResidualRoots() {
					ba := rr.GetApplication()
					residuals = append(residuals, engine.Residual{Element: ba.GetElement()})
					if len(ba.GetElement()) == 0 {
						slog.LogAttrs(context.TODO(), slog.LevelError, "returned empty residual application", slog.Any("bundle", rb))
						panic("sdk returned empty residual application")
					}
					// TODO what happens to output watermarks on splits?
				}
				if len(sr.GetChannelSplits()) != 1 {
					slog.Warn("received non-single channel split", "bundle", rb)
				}
				cs := sr.GetChannelSplits()[0]
				fr := cs.GetFirstResidualElement()
				// The first residual can be after the end of data, so filter out those cases.
				if b.EstimatedInputElements >= int(fr) {
					b.EstimatedInputElements = int(fr) // Update the estimate for the next split.
					// Split Residuals are returned right away for rescheduling.
					em.ReturnResiduals(rb, int(fr), s.inputInfo, engine.Residuals{
						Data: residuals,
					})
				}

				// Any split means we're processing slower than desired, but splitting should increase
				// throughput. Back off for this and other bundles for this stage
				baseTime := s.baseProgTick.Load().(time.Duration)
				newTime := clampTick(baseTime * 4)
				if s.baseProgTick.CompareAndSwap(baseTime, newTime) {
					progTick.Reset(newTime)
				} else {
					progTick.Reset(s.baseProgTick.Load().(time.Duration))
				}
			} else {
				previousIndex = index["index"]
				previousTotalCount = index["totalCount"]
			}
		}
	}
	// If we never received any progress ticks, we may have too long a time, shrink it for new runs instead.
	if !ticked {
		newTick := clampTick(baseTick - minimumProgTick)
		// If it's otherwise unchanged, apply the new duration.
		s.baseProgTick.CompareAndSwap(baseTick, newTick)
	}
	// Tentative Data is ready, commit it to the main datastore.
	slog.Debug("Execute: committing data", "bundle", rb, slog.Any("outputsWithData", maps.Keys(b.OutputData.Raw)), slog.Any("outputs", maps.Keys(s.OutputsToCoders)))

	// Tally metrics immeadiately so they're available before
	// pipeline termination.
	unknownIDs := j.ContributeFinalMetrics(resp)
	if len(unknownIDs) > 0 {
		md := wk.MonitoringMetadata(ctx, unknownIDs)
		j.AddMetricShortIDs(md)
	}

	// ProcessContinuation residuals are rescheduled after the specified delay.
	residuals := engine.Residuals{
		MinOutputWatermarks: map[string]mtime.Time{},
	}
	for _, rr := range resp.GetResidualRoots() {
		ba := rr.GetApplication()
		if len(ba.GetElement()) == 0 {
			slog.LogAttrs(context.TODO(), slog.LevelError, "returned empty residual application", slog.Any("bundle", rb))
			panic("sdk returned empty residual application")
		}
		if residuals.TransformID == "" {
			residuals.TransformID = ba.GetTransformId()
		}
		if residuals.InputID == "" {
			residuals.InputID = ba.GetInputId()
		}
		if residuals.TransformID != ba.GetTransformId() {
			panic("sdk returned inconsistent residual application transform : got = " + ba.GetTransformId() + " want = " + residuals.TransformID)
		}
		if residuals.InputID != ba.GetInputId() {
			panic("sdk returned inconsistent residual application input : got = " + ba.GetInputId() + " want = " + residuals.InputID)
		}

		for col, wm := range ba.GetOutputWatermarks() {
			cur, ok := residuals.MinOutputWatermarks[col]
			if !ok {
				cur = mtime.MaxTimestamp
			}
			residuals.MinOutputWatermarks[col] = mtime.Min(mtime.FromTime(wm.AsTime()), cur)
		}

		residuals.Data = append(residuals.Data, engine.Residual{
			Element: ba.GetElement(),
			Delay:   rr.GetRequestedTimeDelay().AsDuration(),
			Bounded: ba.GetIsBounded() == pipepb.IsBounded_BOUNDED,
		})
	}
	if l := len(residuals.Data); l == 0 {
		slog.Debug("returned empty residual application", "bundle", rb, slog.Int("numResiduals", l), slog.String("pcollection", s.primaryInput))
	}
	em.PersistBundle(rb, s.OutputsToCoders, b.OutputData, s.inputInfo, residuals)
	if s.finalize {
		_, err := b.Finalize(ctx, wk)
		if err != nil {
			slog.Error("SDK Error from bundle finalization", "bundle", rb, "error", err.Error())
			panic(err)
		}
		slog.Info("finalized bundle", "bundle", rb)
	}
	b.OutputData = engine.TentativeData{} // Clear the data.
	return nil
}

func getSideInputs(t *pipepb.PTransform) (map[string]*pipepb.SideInput, error) {
	switch t.GetSpec().GetUrn() {
	case urns.TransformParDo, urns.TransformProcessSizedElements, urns.TransformPairWithRestriction, urns.TransformSplitAndSize, urns.TransformTruncate:
		// Intentionally empty since these are permitted to have side inputs.
	default:
		// Nothing else is allowed to have side inputs.
		return nil, nil
	}
	// TODO, memoize this, so we don't need to repeatedly unmarshal.
	pardo := &pipepb.ParDoPayload{}
	if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
		return nil, fmt.Errorf("unable to decode ParDoPayload")
	}
	return pardo.GetSideInputs(), nil
}

func portFor(wInCid string, wk *worker.W) []byte {
	sourcePort := &fnpb.RemoteGrpcPort{
		CoderId: wInCid,
		ApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
	}
	sourcePortBytes, err := proto.Marshal(sourcePort)
	if err != nil {
		slog.Error("bad port", slog.Any("error", err), slog.String("endpoint", sourcePort.ApiServiceDescriptor.GetUrl()))
	}
	return sourcePortBytes
}

// buildDescriptor constructs a ProcessBundleDescriptor for bundles of this stage.
//
// Requirements:
// * The set of inputs to the stage only include one parallel input.
// * The side input pcollections are fully qualified with global pcollection ID, ingesting transform, and local inputID.
// * The outputs are fully qualified with global PCollectionID, producing transform, and local outputID.
//
// It assumes that the side inputs are not sourced from PCollections generated by any transform in this stage.
//
// Because we need the local ids for routing the sources/sinks information.
func buildDescriptor(stg *stage, comps *pipepb.Components, wk *worker.W, em *engine.ElementManager) (err error) {
	// Catch construction time panics and produce them as errors out.
	defer func() {
		if r := recover(); r != nil {
			switch rt := r.(type) {
			case error:
				err = rt
			default:
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	// Assume stage has an indicated primary input

	coders := map[string]*pipepb.Coder{}
	transforms := map[string]*pipepb.PTransform{}
	pcollections := map[string]*pipepb.PCollection{}

	clonePColToBundle := func(pid string) *pipepb.PCollection {
		col := proto.Clone(comps.GetPcollections()[pid]).(*pipepb.PCollection)
		pcollections[pid] = col
		return col
	}

	// Update coders for Stateful transforms.
	for _, tid := range stg.transforms {
		t := comps.GetTransforms()[tid]

		transforms[tid] = t

		if t.GetSpec().GetUrn() != urns.TransformParDo {
			continue
		}

		pardo := &pipepb.ParDoPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(t.GetSpec().GetPayload(), pardo); err != nil {
			return fmt.Errorf("unable to decode ParDoPayload for %v in stage %v", tid, stg.ID)
		}

		// We need to ensure the coders can be handled by prism, and are available in the bundle descriptor.
		// So we rewrite the transform's Payload with updated coder ids here.
		var rewrite bool
		var rewriteErr error
		for stateID, s := range pardo.GetStateSpecs() {
			rewrite = true
			rewriteCoder := func(cid *string) {
				newCid, err := lpUnknownCoders(*cid, coders, comps.GetCoders())
				if err != nil {
					rewriteErr = fmt.Errorf("unable to rewrite coder %v for state %v for transform %v in stage %v:%w", *cid, stateID, tid, stg.ID, err)
					return
				}
				*cid = newCid
			}
			switch s := s.GetSpec().(type) {
			case *pipepb.StateSpec_BagSpec:
				rewriteCoder(&s.BagSpec.ElementCoderId)
			case *pipepb.StateSpec_SetSpec:
				rewriteCoder(&s.SetSpec.ElementCoderId)
			case *pipepb.StateSpec_OrderedListSpec:
				rewriteCoder(&s.OrderedListSpec.ElementCoderId)
				// Add the length determination helper for OrderedList state values.
				if stg.stateTypeLen == nil {
					stg.stateTypeLen = map[engine.LinkID]func([]byte) int{}
				}
				linkID := engine.LinkID{
					Transform: tid,
					Local:     stateID,
				}
				var fn func([]byte) int
				switch v := coders[s.OrderedListSpec.GetElementCoderId()]; v.GetSpec().GetUrn() {
				case urns.CoderBool:
					fn = func(_ []byte) int {
						return 1
					}
				case urns.CoderDouble:
					fn = func(_ []byte) int {
						return 8
					}
				case urns.CoderVarInt:
					fn = func(b []byte) int {
						_, n := protowire.ConsumeVarint(b)
						return int(n)
					}
				case urns.CoderLengthPrefix, urns.CoderBytes, urns.CoderStringUTF8:
					fn = func(b []byte) int {
						l, n := protowire.ConsumeVarint(b)
						return int(l) + n
					}
				default:
					rewriteErr = fmt.Errorf("unknown coder used for ordered list state after re-write id: %v coder: %v, for state %v for transform %v in stage %v", s.OrderedListSpec.GetElementCoderId(), v, stateID, tid, stg.ID)
				}
				stg.stateTypeLen[linkID] = fn
			case *pipepb.StateSpec_CombiningSpec:
				rewriteCoder(&s.CombiningSpec.AccumulatorCoderId)
			case *pipepb.StateSpec_MapSpec:
				rewriteCoder(&s.MapSpec.KeyCoderId)
				rewriteCoder(&s.MapSpec.ValueCoderId)
			case *pipepb.StateSpec_MultimapSpec:
				rewriteCoder(&s.MultimapSpec.KeyCoderId)
				rewriteCoder(&s.MultimapSpec.ValueCoderId)
			case *pipepb.StateSpec_ReadModifyWriteSpec:
				rewriteCoder(&s.ReadModifyWriteSpec.CoderId)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}
		for timerID, v := range pardo.GetTimerFamilySpecs() {
			stg.hasTimers = append(stg.hasTimers, engine.StaticTimerID{TransformID: tid, TimerFamily: timerID})
			if v.TimeDomain == pipepb.TimeDomain_PROCESSING_TIME {
				if stg.processingTimeTimers == nil {
					stg.processingTimeTimers = map[string]bool{}
				}
				stg.processingTimeTimers[timerID] = true
			}
			rewrite = true
			newCid, err := lpUnknownCoders(v.GetTimerFamilyCoderId(), coders, comps.GetCoders())
			if err != nil {
				return fmt.Errorf("unable to rewrite coder %v for timer %v for transform %v in stage %v: %w", v.GetTimerFamilyCoderId(), timerID, tid, stg.ID, err)
			}
			v.TimerFamilyCoderId = newCid
		}
		if rewrite {
			pyld, err := proto.MarshalOptions{}.Marshal(pardo)
			if err != nil {
				return fmt.Errorf("unable to encode ParDoPayload for %v in stage %v after rewrite", tid, stg.ID)
			}
			t.Spec.Payload = pyld
		}
	}
	if len(transforms) == 0 {
		return fmt.Errorf("buildDescriptor: invalid stage - no transforms at all %v", stg.ID)
	}

	// Start with outputs, since they're simple and uniform.
	sink2Col := map[string]string{}
	col2Coders := map[string]engine.PColInfo{}
	for _, o := range stg.outputs {
		col := clonePColToBundle(o.Global)
		wOutCid, err := makeWindowedValueCoder(o.Global, comps, coders)
		if err != nil {
			return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for output %+v, pcol %q %v:\n%w %v", stg.ID, o, o.Global, prototext.Format(col), err, stg.transforms)
		}
		sinkID := o.Transform + "_" + o.Local
		ed := collectionPullDecoder(col.GetCoderId(), coders, comps)

		var kd func(io.Reader) []byte
		if kcid, ok := extractKVCoderID(col.GetCoderId(), coders); ok {
			kd = collectionPullDecoder(kcid, coders, comps)
		}

		winCoder, wDec, wEnc := getWindowValueCoders(comps, col, coders)
		sink2Col[sinkID] = o.Global
		col2Coders[o.Global] = engine.PColInfo{
			GlobalID:    o.Global,
			WindowCoder: winCoder,
			WDec:        wDec,
			WEnc:        wEnc,
			EDec:        ed,
			KeyDec:      kd,
		}
		transforms[sinkID] = sinkTransform(sinkID, portFor(wOutCid, wk), o.Global)
	}

	var prepareSides []func(b *worker.B, watermark mtime.Time)
	for _, si := range stg.sideInputs {
		col := clonePColToBundle(si.Global)

		oCID := col.GetCoderId()
		nCID, err := lpUnknownCoders(oCID, coders, comps.GetCoders())
		if err != nil {
			return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for side input %+v, pcol %q %v:\n%w", stg.ID, si, si.Global, prototext.Format(col), err)
		}
		if oCID != nCID {
			// Add a synthetic PCollection set with the new coder.
			newGlobal := si.Global + "_prismside"
			pcollections[newGlobal] = &pipepb.PCollection{
				DisplayData:         col.GetDisplayData(),
				UniqueName:          col.GetUniqueName(),
				CoderId:             nCID,
				IsBounded:           col.GetIsBounded(),
				WindowingStrategyId: col.WindowingStrategyId,
			}
			// Update side inputs to point to new PCollection with any replaced coders.
			transforms[si.Transform].GetInputs()[si.Local] = newGlobal
			// TODO: replace si.Global with newGlobal?
		}
		prepSide, err := handleSideInput(si, comps, transforms, pcollections, coders, em)
		if err != nil {
			slog.Error("buildDescriptor: handleSideInputs", "error", err, slog.String("transformID", si.Transform))
			return err
		}
		prepareSides = append(prepareSides, prepSide)
	}

	// Finally, the parallel input, which is it's own special snowflake, that needs a datasource.
	// This id is directly used for the source, but this also copies
	// coders used by side inputs to the coders map for the bundle, so
	// needs to be run for every ID.

	col := clonePColToBundle(stg.primaryInput)
	if newCID, err := lpUnknownCoders(col.GetCoderId(), coders, comps.GetCoders()); err == nil && col.GetCoderId() != newCID {
		col.CoderId = newCID
	} else if err != nil {
		return fmt.Errorf("buildDescriptor: couldn't rewrite coder %q for primary input pcollection %q: %w", col.GetCoderId(), stg.primaryInput, err)
	}

	wInCid, err := makeWindowedValueCoder(stg.primaryInput, comps, coders)
	if err != nil {
		return fmt.Errorf("buildDescriptor: failed to handle coder on stage %v for primary input, pcol %q %v:\n%w\n%v", stg.ID, stg.primaryInput, prototext.Format(col), err, stg.transforms)
	}
	ed := collectionPullDecoder(col.GetCoderId(), coders, comps)
	winCoder, wDec, wEnc := getWindowValueCoders(comps, col, coders)

	var kd func(io.Reader) []byte
	if kcid, ok := extractKVCoderID(col.GetCoderId(), coders); ok {
		kd = collectionPullDecoder(kcid, coders, comps)
	}

	inputInfo := engine.PColInfo{
		GlobalID:    stg.primaryInput,
		WindowCoder: winCoder,
		WDec:        wDec,
		WEnc:        wEnc,
		EDec:        ed,
		KeyDec:      kd,
	}

	stg.inputTransformID = stg.ID + "_source"
	transforms[stg.inputTransformID] = sourceTransform(stg.inputTransformID, portFor(wInCid, wk), stg.primaryInput)

	// Update coders for internal collections, and add those collections to the bundle descriptor.
	for _, pid := range stg.internalCols {
		col := clonePColToBundle(pid)
		if newCID, err := lpUnknownCoders(col.GetCoderId(), coders, comps.GetCoders()); err == nil && col.GetCoderId() != newCID {
			col.CoderId = newCID
		} else if err != nil {
			return fmt.Errorf("buildDescriptor: coder  couldn't rewrite coder %q for internal pcollection %q: %w", col.GetCoderId(), pid, err)
		}
	}
	// Add coders for all windowing strategies.
	// TODO: filter PCollections, filter windowing strategies by Pcollections instead.
	for _, ws := range comps.GetWindowingStrategies() {
		lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
	}

	reconcileCoders(coders, comps.GetCoders())

	var timerServiceDescriptor *pipepb.ApiServiceDescriptor
	if len(stg.hasTimers) > 0 {
		timerServiceDescriptor = &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		}
	}

	desc := &fnpb.ProcessBundleDescriptor{
		Id:                  stg.ID,
		Transforms:          transforms,
		WindowingStrategies: comps.GetWindowingStrategies(),
		Pcollections:        pcollections,
		Coders:              coders,
		StateApiServiceDescriptor: &pipepb.ApiServiceDescriptor{
			Url: wk.Endpoint(),
		},
		TimerApiServiceDescriptor: timerServiceDescriptor,
	}

	stg.desc = desc
	stg.prepareSides = func(b *worker.B, watermark mtime.Time) {
		for _, prep := range prepareSides {
			prep(b, watermark)
		}
	}
	stg.SinkToPCollection = sink2Col
	stg.OutputsToCoders = col2Coders
	stg.inputInfo = inputInfo

	wk.Descriptors[stg.ID] = stg.desc
	return nil
}

// handleSideInput returns a closure that will look up the data for a side input appropriate for the given watermark.
func handleSideInput(link engine.LinkID, comps *pipepb.Components, transforms map[string]*pipepb.PTransform, pcols map[string]*pipepb.PCollection, coders map[string]*pipepb.Coder, em *engine.ElementManager) (func(b *worker.B, watermark mtime.Time), error) {
	t := transforms[link.Transform]
	sis, err := getSideInputs(t)
	if err != nil {
		return nil, err
	}

	switch si := sis[link.Local]; si.GetAccessPattern().GetUrn() {
	case urns.SideInputIterable:
		slog.Debug("urnSideInputIterable",
			slog.String("sourceTransform", t.GetUniqueName()),
			slog.String("local", link.Local),
			slog.String("global", link.Global))

		col := pcols[link.Global]
		// The returned coders are unused here, but they add the side input coders
		// to the stage components for use SDK side.

		collectionPullDecoder(col.GetCoderId(), coders, comps)
		getWindowValueCoders(comps, col, coders)
		// May be of zero length, but that's OK. Side inputs can be emp
		return func(b *worker.B, watermark mtime.Time) {
			// May be of zero length, but that's OK. Side inputs can be empty.
			data := em.GetSideData(b.PBDID, link.Transform, link.Local, watermark)
			if b.IterableSideInputData == nil {
				b.IterableSideInputData = map[worker.SideInputKey]map[typex.Window][][]byte{}
			}
			b.IterableSideInputData[worker.SideInputKey{
				TransformID: link.Transform,
				Local:       link.Local,
			}] = data
		}, nil

	case urns.SideInputMultiMap:
		slog.Debug("urnSideInputMultiMap",
			slog.String("sourceTransform", t.GetUniqueName()),
			slog.String("local", link.Local),
			slog.String("global", link.Global))
		col := pcols[link.Global]

		kvc := comps.GetCoders()[col.GetCoderId()]
		if kvc.GetSpec().GetUrn() != urns.CoderKV {
			return nil, fmt.Errorf("multimap side inputs needs KV coder, got %v", kvc.GetSpec().GetUrn())
		}

		kd := collectionPullDecoder(kvc.GetComponentCoderIds()[0], coders, comps)
		vd := collectionPullDecoder(kvc.GetComponentCoderIds()[1], coders, comps)

		// The returned coders are unused here, but they add the side input coders
		// to the stage components for use SDK side.
		getWindowValueCoders(comps, col, coders)
		return func(b *worker.B, watermark mtime.Time) {
			// May be of zero length, but that's OK. Side inputs can be empty.
			data := em.GetSideData(b.PBDID, link.Transform, link.Local, watermark)
			if b.MultiMapSideInputData == nil {
				b.MultiMapSideInputData = map[worker.SideInputKey]map[typex.Window]map[string][][]byte{}
			}

			windowed := map[typex.Window]map[string][][]byte{}
			for win, ds := range data {
				if len(ds) == 0 {
					continue
				}
				byKey := map[string][][]byte{}
				for _, datum := range ds {
					r := bytes.NewBuffer(datum)
					kb := kd(r)
					byKey[string(kb)] = append(byKey[string(kb)], vd(r))
				}
				windowed[win] = byKey
			}
			b.MultiMapSideInputData[worker.SideInputKey{
				TransformID: link.Transform,
				Local:       link.Local,
			}] = windowed
		}, nil
	default:
		return nil, fmt.Errorf("local input %v (global %v) uses accesspattern %v", link.Local, link.Global, prototext.Format(si.GetAccessPattern()))
	}
}

func sourceTransform(parentID string, sourcePortBytes []byte, outPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: parentID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urns.TransformSource,
			Payload: sourcePortBytes,
		},
		Outputs: map[string]string{
			"i0": outPID,
		},
	}
	return source
}

func sinkTransform(sinkID string, sinkPortBytes []byte, inPID string) *pipepb.PTransform {
	source := &pipepb.PTransform{
		UniqueName: sinkID,
		Spec: &pipepb.FunctionSpec{
			Urn:     urns.TransformSink,
			Payload: sinkPortBytes,
		},
		Inputs: map[string]string{
			"i0": inPID,
		},
	}
	return source
}
