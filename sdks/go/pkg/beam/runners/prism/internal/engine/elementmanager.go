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

// Package engine handles the operational components of a runner, to
// track elements, watermarks, timers, triggers etc
package engine

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slog"
)

type element struct {
	window                 typex.Window
	timestamp              mtime.Time
	holdTimestamp          mtime.Time // only used for Timers
	pane                   typex.PaneInfo
	transform, family, tag string // only used for Timers.

	elmBytes []byte // When nil, indicates this is a timer.
	keyBytes []byte
}

func (e *element) IsTimer() bool {
	return e.elmBytes == nil
}

func (e *element) IsData() bool {
	return !e.IsTimer()
}

func (e element) String() string {
	if e.IsTimer() {
		return fmt.Sprintf("{Timer - Window %v, EventTime %v, Hold %v, %q %q %q %q}", e.window, e.timestamp, e.holdTimestamp, e.transform, e.family, e.tag, e.keyBytes)
	}
	return fmt.Sprintf("{Data - Window %v, EventTime %v, Element %v}", e.window, e.timestamp, e.elmBytes)
}

type elements struct {
	es           []element
	minTimestamp mtime.Time
}

type PColInfo struct {
	GlobalID string
	WDec     exec.WindowDecoder
	WEnc     exec.WindowEncoder
	EDec     func(io.Reader) []byte
	KeyDec   func(io.Reader) []byte
}

// ToData recodes the elements with their approprate windowed value header.
func (es elements) ToData(info PColInfo) [][]byte {
	var ret [][]byte
	for _, e := range es.es {
		var buf bytes.Buffer
		exec.EncodeWindowedValueHeader(info.WEnc, []typex.Window{e.window}, e.timestamp, e.pane, &buf)
		buf.Write(e.elmBytes)
		ret = append(ret, buf.Bytes())
	}
	return ret
}

// elementHeap orders elements based on their timestamps
// so we can always find the minimum timestamp of pending elements.
type elementHeap []element

func (h elementHeap) Len() int { return len(h) }
func (h elementHeap) Less(i, j int) bool {
	// If the timestamps are the same, data comes before timers.
	if h[i].timestamp == h[j].timestamp {
		if h[i].IsTimer() && h[j].IsData() {
			return false // j before i
		} else if h[i].IsData() && h[j].IsTimer() {
			return true // i before j.
		}
		// They're the same kind, fall through to timestamp less for consistency.
	}
	// Otherwise compare by timestamp.
	return h[i].timestamp < h[j].timestamp
}
func (h elementHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *elementHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(element))
}

func (h *elementHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Config struct {
	// MaxBundleSize caps the number of elements permitted in a bundle.
	// 0 or less means this is ignored.
	MaxBundleSize int
}

// ElementManager handles elements, watermarks, and related errata to determine
// if a stage is able to be executed. It is the core execution engine of Prism.
//
// Essentially, it needs to track the current watermarks for each PCollection
// and transform/stage. But it's tricky, since the watermarks for the
// PCollections are always relative to transforms/stages.
//
// Key parts:
//
//   - The parallel input's PCollection's watermark is relative to committed consumed
//     elements. That is, the input elements consumed by the transform after a successful
//     bundle, can advance the watermark, based on the minimum of their elements.
//   - An output PCollection's watermark is relative to its producing transform,
//     which relates to *all of it's outputs*.
//
// This means that a PCollection's watermark is the minimum of all it's consuming transforms.
//
// So, the watermark manager needs to track:
// Pending Elements for each stage, along with their windows and timestamps.
// Each transform's view of the watermarks for the PCollections.
//
// Watermarks are advanced based on consumed input, except if the stage produces residuals.
type ElementManager struct {
	config Config

	stages map[string]*stageState // The state for each stage.

	consumers     map[string][]string // Map from pcollectionID to stageIDs that consumes them as primary input.
	sideConsumers map[string][]LinkID // Map from pcollectionID to the stage+transform+input that consumes them as side input.

	pcolParents map[string]string // Map from pcollectionID to stageIDs that produce the pcollection.

	refreshCond        sync.Cond   // refreshCond protects the following fields with it's lock, and unblocks bundle scheduling.
	inprogressBundles  set[string] // Active bundleIDs
	watermarkRefreshes set[string] // Scheduled stageID watermark refreshes

	livePending     atomic.Int64   // An accessible live pending count. DEBUG USE ONLY
	pendingElements sync.WaitGroup // pendingElements counts all unprocessed elements in a job. Jobs with no pending elements terminate successfully.
}

func (em *ElementManager) addPending(v int) {
	em.livePending.Add(int64(v))
	em.pendingElements.Add(v)
}

// LinkID represents a fully qualified input or output.
type LinkID struct {
	Transform, Local, Global string
}

func NewElementManager(config Config) *ElementManager {
	return &ElementManager{
		config:             config,
		stages:             map[string]*stageState{},
		consumers:          map[string][]string{},
		sideConsumers:      map[string][]LinkID{},
		pcolParents:        map[string]string{},
		watermarkRefreshes: set[string]{},
		inprogressBundles:  set[string]{},
		refreshCond:        sync.Cond{L: &sync.Mutex{}},
	}
}

// AddStage adds a stage to this element manager, connecting it's PCollections and
// nodes to the watermark propagation graph.
func (em *ElementManager) AddStage(ID string, inputIDs, outputIDs []string, sides []LinkID) {
	slog.Debug("AddStage", slog.String("ID", ID), slog.Any("inputs", inputIDs), slog.Any("sides", sides), slog.Any("outputs", outputIDs))
	ss := makeStageState(ID, inputIDs, outputIDs, sides)

	em.stages[ss.ID] = ss
	for _, outputID := range ss.outputIDs {
		em.pcolParents[outputID] = ss.ID
	}
	for _, input := range inputIDs {
		em.consumers[input] = append(em.consumers[input], ss.ID)
	}
	for _, side := range ss.sides {
		// Note that we use the StageID as the global ID in the value since we need
		// to be able to look up the consuming stage, from the global PCollectionID.
		em.sideConsumers[side.Global] = append(em.sideConsumers[side.Global], LinkID{Global: ss.ID, Local: side.Local, Transform: side.Transform})
	}
}

// StageAggregates marks the given stage as an aggregation, which
// means elements will only be processed based on windowing strategies.
func (em *ElementManager) StageAggregates(ID string) {
	em.stages[ID].aggregate = true
}

// StageStateful marks the given stage as stateful, which means elements are
// processed by key.
func (em *ElementManager) StageStateful(ID string) {
	em.stages[ID].stateful = true
}

// Impulse marks and initializes the given stage as an impulse which
// is a root transform that starts processing.
func (em *ElementManager) Impulse(stageID string) {
	stage := em.stages[stageID]
	newPending := []element{{
		window:    window.GlobalWindow{},
		timestamp: mtime.MinTimestamp,
		pane:      typex.NoFiringPane(),
		elmBytes:  []byte{0}, // Represents an encoded 0 length byte slice.
	}}

	consumers := em.consumers[stage.outputIDs[0]]
	slog.Debug("Impulse", slog.String("stageID", stageID), slog.Any("outputs", stage.outputIDs), slog.Any("consumers", consumers))

	for _, sID := range consumers {
		consumer := em.stages[sID]
		count := consumer.AddPending(newPending)
		em.addPending(count)
	}
	refreshes := stage.updateWatermarks(em)
	em.addRefreshes(refreshes)
}

type RunBundle struct {
	StageID   string
	BundleID  string
	Watermark mtime.Time
}

func (rb RunBundle) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("ID", rb.BundleID),
		slog.String("stage", rb.StageID),
		slog.Time("watermark", rb.Watermark.ToTime()))
}

// Bundles is the core execution loop. It produces a sequences of bundles able to be executed.
// The returned channel is closed when the context is canceled, or there are no pending elements
// remaining.
func (em *ElementManager) Bundles(ctx context.Context, nextBundID func() string) <-chan RunBundle {
	runStageCh := make(chan RunBundle)
	ctx, cancelFn := context.WithCancelCause(ctx)
	go func() {
		em.pendingElements.Wait()
		slog.Debug("no more pending elements: terminating pipeline")
		cancelFn(fmt.Errorf("elementManager out of elements, cleaning up"))
		// Ensure the watermark evaluation goroutine exits.
		em.refreshCond.Broadcast()
	}()
	// Watermark evaluation goroutine.
	go func() {
		defer close(runStageCh)
		for {
			em.refreshCond.L.Lock()
			// If there are no watermark refreshes available, we wait until there are.
			for len(em.watermarkRefreshes) == 0 {
				// Check to see if we must exit
				select {
				case <-ctx.Done():
					em.refreshCond.L.Unlock()
					return
				default:
				}
				em.refreshCond.Wait() // until watermarks may have changed.
			}

			// We know there is some work we can do that may advance the watermarks,
			// refresh them, and see which stages have advanced.
			advanced := em.refreshWatermarks()

			// Check each advanced stage, to see if it's able to execute based on the watermark.
			for stageID := range advanced {
				ss := em.stages[stageID]
				watermark, ready := ss.bundleReady(em)
				if ready {
					bundleID, ok, reschedule := ss.startBundle(watermark, nextBundID)
					// Handle the reschedule even when there's no bundle.
					if reschedule {
						em.watermarkRefreshes.insert(stageID)
					}
					if !ok {
						continue
					}
					rb := RunBundle{StageID: stageID, BundleID: bundleID, Watermark: watermark}

					em.inprogressBundles.insert(rb.BundleID)
					em.refreshCond.L.Unlock()

					select {
					case <-ctx.Done():
						return
					case runStageCh <- rb:
					}
					em.refreshCond.L.Lock()
				}
			}
			if len(em.inprogressBundles) == 0 && len(em.watermarkRefreshes) == 0 {
				v := em.livePending.Load()
				slog.Debug("Bundles: nothing in progress and no refreshes", slog.Int64("pendingElementCount", v))
				if v > 0 {
					var stageState []string
					ids := maps.Keys(em.stages)
					sort.Strings(ids)
					for _, id := range ids {
						ss := em.stages[id]
						inW := ss.InputWatermark()
						outW := ss.OutputWatermark()
						upPCol, upW := ss.UpstreamWatermark()
						upS := em.pcolParents[upPCol]
						stageState = append(stageState, fmt.Sprintln(id, "watermark in", inW, "out", outW, "upstream", upW, "from", upS, "pending", ss.pending, "byKey", ss.pendingByKeys, "inprogressKeys", ss.inprogressKeys, "byBundle", ss.inprogressKeysByBundle, "holds", ss.watermarkHoldHeap, "holdCounts", ss.watermarkHoldsCounts))
					}
					panic(fmt.Sprintf("nothing in progress and no refreshes with non zero pending elements: %v\n%v", v, strings.Join(stageState, "")))
				}
			} else if len(em.inprogressBundles) == 0 {
				v := em.livePending.Load()
				slog.Debug("Bundles: nothing in progress after advance",
					slog.Any("advanced", advanced),
					slog.Int("refreshCount", len(em.watermarkRefreshes)),
					slog.Int64("pendingElementCount", v),
				)
			}
			em.refreshCond.L.Unlock()
		}
	}()
	return runStageCh
}

// InputForBundle returns pre-allocated data for the given bundle, encoding the elements using
// the PCollection's coders.
func (em *ElementManager) InputForBundle(rb RunBundle, info PColInfo) [][]byte {
	ss := em.stages[rb.StageID]
	ss.mu.Lock()
	defer ss.mu.Unlock()
	es := ss.inprogress[rb.BundleID]
	return es.ToData(info)
}

// DataAndTimerInputForBundle returns pre-allocated data for the given bundle and the estimated number of elements.
// Elements are encoded with the PCollection's coders.
func (em *ElementManager) DataAndTimerInputForBundle(rb RunBundle, info PColInfo) ([]*Block, int) {
	ss := em.stages[rb.StageID]
	ss.mu.Lock()
	defer ss.mu.Unlock()
	es := ss.inprogress[rb.BundleID]

	var total int

	var ret []*Block
	cur := &Block{}
	for _, e := range es.es {
		switch {
		case e.IsTimer() && (cur.Kind != BlockTimer || e.family != cur.Family || cur.Transform != e.transform):
			total += len(cur.Bytes)
			cur = &Block{
				Kind:      BlockTimer,
				Transform: e.transform,
				Family:    e.family,
			}
			ret = append(ret, cur)
			fallthrough
		case e.IsTimer() && cur.Kind == BlockTimer:
			var buf bytes.Buffer
			// Key
			buf.Write(e.keyBytes) // Includes the length prefix if any.
			// Tag
			coder.EncodeVarInt(int64(len(e.tag)), &buf)
			buf.WriteString(e.tag)
			// Windows
			info.WEnc.Encode([]typex.Window{e.window}, &buf)
			// Clear
			buf.Write([]byte{0})
			// Firing timestamp
			coder.EncodeEventTime(e.timestamp, &buf)
			// Hold timestamp
			coder.EncodeEventTime(e.holdTimestamp, &buf)
			// Pane
			coder.EncodePane(e.pane, &buf)

			cur.Bytes = append(cur.Bytes, buf.Bytes())
		case cur.Kind != BlockData:
			total += len(cur.Bytes)
			cur = &Block{
				Kind: BlockData,
			}
			ret = append(ret, cur)
			fallthrough
		default:
			var buf bytes.Buffer
			exec.EncodeWindowedValueHeader(info.WEnc, []typex.Window{e.window}, e.timestamp, e.pane, &buf)
			buf.Write(e.elmBytes)
			cur.Bytes = append(cur.Bytes, buf.Bytes())
		}
	}
	total += len(cur.Bytes)
	return ret, total
}

// BlockKind indicates how the block is to be handled.
type BlockKind int32

const (
	blockUnset BlockKind = iota // blockUnset
	BlockData                   // BlockData represents data for the bundle.
	BlockTimer                  // BlockTimer represents timers for the bundle.
)

type Block struct {
	Kind              BlockKind
	Bytes             [][]byte
	Transform, Family string
}

// StateForBundle retreives relevant state for the given bundle, WRT the data in the bundle.
//
// TODO(lostluck): Consider unifiying with InputForBundle, to reduce lock contention.
func (em *ElementManager) StateForBundle(rb RunBundle) TentativeData {
	ss := em.stages[rb.StageID]
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var ret TentativeData
	keys := ss.inprogressKeysByBundle[rb.BundleID]
	// TODO(lostluck): Also track windows per bundle, to reduce copying.
	if len(ss.state) > 0 {
		ret.state = map[LinkID]map[typex.Window]map[string]StateData{}
	}
	for link, winMap := range ss.state {
		for w, keyMap := range winMap {
			for key := range keys {
				data, ok := keyMap[key]
				if !ok {
					continue
				}
				linkMap, ok := ret.state[link]
				if !ok {
					linkMap = map[typex.Window]map[string]StateData{}
					ret.state[link] = linkMap
				}
				wlinkMap, ok := linkMap[w]
				if !ok {
					wlinkMap = map[string]StateData{}
					linkMap[w] = wlinkMap
				}
				var mm map[string][][]byte
				if len(data.Multimap) > 0 {
					mm = map[string][][]byte{}
					for uk, v := range data.Multimap {
						// Clone the "holding" slice, but refer to the existing data bytes.
						mm[uk] = append([][]byte(nil), v...)
					}
				}
				// Clone the "holding" slice, but refer to the existing data bytes.
				wlinkMap[key] = StateData{
					Bag:      append([][]byte(nil), data.Bag...),
					Multimap: mm,
				}
			}
		}
	}

	return ret
}

// reElementResiduals extracts the windowed value header from residual bytes, and explodes them
// back out to their windows.
func reElementResiduals(residuals [][]byte, inputInfo PColInfo, rb RunBundle) []element {
	var unprocessedElements []element
	for _, residual := range residuals {
		buf := bytes.NewBuffer(residual)
		ws, et, pn, err := exec.DecodeWindowedValueHeader(inputInfo.WDec, buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("reElementResiduals: error decoding residual header", "error", err, "bundle", rb)
			panic("error decoding residual header:" + err.Error())
		}
		if len(ws) == 0 {
			slog.Error("reElementResiduals: sdk provided a windowed value header 0 windows", "bundle", rb)
			panic("error decoding residual header: sdk provided a windowed value header 0 windows")
		}
		// POSSIBLY BAD PATTERN: The buffer is invalidated on the next call, which doesn't always happen.
		// But the decoder won't be mutating the buffer bytes, just reading the data. So the elmBytes
		// should remain pointing to the whole element, and we should have a copy of the key bytes.
		// Ideally, we're simply refering to the key part of the existing buffer.
		elmBytes := buf.Bytes()
		var keyBytes []byte
		if inputInfo.KeyDec != nil {
			keyBytes = inputInfo.KeyDec(buf)
		}

		for _, w := range ws {
			unprocessedElements = append(unprocessedElements,
				element{
					window:    w,
					timestamp: et,
					pane:      pn,
					elmBytes:  elmBytes,
					keyBytes:  keyBytes,
				})
		}
	}
	return unprocessedElements
}

// PersistBundle uses the tentative bundle output to update the watermarks for the stage.
// Each stage has two monotonically increasing watermarks, the input watermark, and the output
// watermark.
//
// MAX(CurrentInputWatermark, MIN(PendingElements, InputPCollectionWatermarks)
// MAX(CurrentOutputWatermark, MIN(InputWatermark, WatermarkHolds))
//
// PersistBundle takes in the stage ID, ID of the bundle associated with the pending
// input elements, and the committed output elements.
func (em *ElementManager) PersistBundle(rb RunBundle, col2Coders map[string]PColInfo, d TentativeData, inputInfo PColInfo, residuals [][]byte, estimatedOWM map[string]mtime.Time) {
	for output, data := range d.Raw {
		info := col2Coders[output]
		var newPending []element
		slog.Debug("PersistBundle: processing output", "bundle", rb, slog.String("output", output))
		for _, datum := range data {
			buf := bytes.NewBuffer(datum)
			if len(datum) == 0 {
				panic(fmt.Sprintf("zero length data for %v: ", output))
			}
			for {
				var rawBytes bytes.Buffer
				tee := io.TeeReader(buf, &rawBytes)
				ws, et, pn, err := exec.DecodeWindowedValueHeader(info.WDec, tee)
				if err != nil {
					if err == io.EOF {
						break
					}
					slog.Error("PersistBundle: error decoding watermarks", "error", err, "bundle", rb, slog.String("output", output))
					panic("error decoding watermarks")
				}
				if len(ws) == 0 {
					slog.Error("PersistBundle: sdk provided a windowed value header 0 windows", "bundle", rb)
					panic("error decoding residual header: sdk provided a windowed value header 0 windows")
				}
				// TODO: Optimize unnecessary copies. This is doubleteeing.
				elmBytes := info.EDec(tee)
				var keyBytes []byte
				if info.KeyDec != nil {
					kbuf := bytes.NewBuffer(elmBytes)
					keyBytes = info.KeyDec(kbuf) // TODO: Optimize unnecessary copies. This is tripleteeing?
				}
				for _, w := range ws {
					newPending = append(newPending,
						element{
							window:    w,
							timestamp: et,
							pane:      pn,
							elmBytes:  elmBytes,
							keyBytes:  keyBytes,
						})
				}
			}
		}
		consumers := em.consumers[output]
		slog.Debug("PersistBundle: bundle has downstream consumers.", "bundle", rb, slog.Int("newPending", len(newPending)), "consumers", consumers)
		for _, sID := range consumers {
			consumer := em.stages[sID]
			count := consumer.AddPending(newPending)
			em.addPending(count)
		}
		sideConsumers := em.sideConsumers[output]
		for _, link := range sideConsumers {
			consumer := em.stages[link.Global]
			consumer.AddPendingSide(newPending, link.Transform, link.Local)
		}
	}

	// Process each timer family in the order we received them, so we can filter to the last one.
	// Since we're process each timer family individually, use a unique key for each userkey, tag, window.
	// The last timer set for each combination is the next one we're keeping.
	type timerKey struct {
		key string
		tag string
		win typex.Window
	}

	var pendingTimers []element
	for tentativeKey, timers := range d.timers {
		keyToTimers := map[timerKey]element{}
		for _, t := range timers {
			key, tag, elms := decodeTimer(inputInfo.KeyDec, true, t)
			for _, e := range elms {
				keyToTimers[timerKey{key: string(key), tag: tag, win: e.window}] = e
			}
			if len(elms) == 0 {
				// TODO(lostluck): Determine best way to mark clear a timer cleared.
				continue
			}
		}

		for _, elm := range keyToTimers {
			elm.transform = tentativeKey.Transform
			elm.family = tentativeKey.Family
			pendingTimers = append(pendingTimers, elm)
		}
	}

	stage := em.stages[rb.StageID]
	if len(pendingTimers) > 0 {
		count := stage.AddPending(pendingTimers)
		em.addPending(count)
	}

	// Return unprocessed to this stage's pending
	unprocessedElements := reElementResiduals(residuals, inputInfo, rb)
	// Add unprocessed back to the pending stack.
	if len(unprocessedElements) > 0 {
		count := stage.AddPending(unprocessedElements)
		em.addPending(count)
	}
	// Clear out the inprogress elements associated with the completed bundle.
	// Must be done after adding the new pending elements to avoid an incorrect
	// watermark advancement.
	stage.mu.Lock()
	completed := stage.inprogress[rb.BundleID]
	em.addPending(-len(completed.es))
	delete(stage.inprogress, rb.BundleID)
	for k := range stage.inprogressKeysByBundle[rb.BundleID] {
		delete(stage.inprogressKeys, k)
	}
	delete(stage.inprogressKeysByBundle, rb.BundleID)

	for hold, v := range stage.inprogressHoldsByBundle[rb.BundleID] {
		n := stage.watermarkHoldsCounts[hold] - v
		if n == 0 {
			delete(stage.watermarkHoldsCounts, hold)
			for i, h := range stage.watermarkHoldHeap {
				if hold == h {
					heap.Remove(&stage.watermarkHoldHeap, i)
					break
				}
			}
		} else {
			stage.watermarkHoldsCounts[hold] = n
		}
	}
	delete(stage.inprogressHoldsByBundle, rb.BundleID)

	// If there are estimated output watermarks, set the estimated
	// output watermark for the stage.
	if len(estimatedOWM) > 0 {
		estimate := mtime.MaxTimestamp
		for _, t := range estimatedOWM {
			estimate = mtime.Min(estimate, t)
		}
		stage.estimatedOutput = estimate
	}

	// Handle persisting.
	for link, winMap := range d.state {
		linkMap, ok := stage.state[link]
		if !ok {
			linkMap = map[typex.Window]map[string]StateData{}
			stage.state[link] = linkMap
		}
		for w, keyMap := range winMap {
			wlinkMap, ok := linkMap[w]
			if !ok {
				wlinkMap = map[string]StateData{}
				linkMap[w] = wlinkMap
			}
			for key, data := range keyMap {
				wlinkMap[key] = data
			}
		}
	}
	stage.mu.Unlock()

	em.addRefreshAndClearBundle(stage.ID, rb.BundleID)
}

// FailBundle clears the extant data allowing the execution to shut down.
func (em *ElementManager) FailBundle(rb RunBundle) {
	stage := em.stages[rb.StageID]
	stage.mu.Lock()
	completed := stage.inprogress[rb.BundleID]
	em.addPending(-len(completed.es))
	delete(stage.inprogress, rb.BundleID)
	stage.mu.Unlock()
	em.addRefreshAndClearBundle(rb.StageID, rb.BundleID)
}

// ReturnResiduals is called after a successful split, so the remaining work
// can be re-assigned to a new bundle.
func (em *ElementManager) ReturnResiduals(rb RunBundle, firstRsIndex int, inputInfo PColInfo, residuals [][]byte) {
	stage := em.stages[rb.StageID]

	stage.splitBundle(rb, firstRsIndex)
	unprocessedElements := reElementResiduals(residuals, inputInfo, rb)
	if len(unprocessedElements) > 0 {
		slog.Debug("ReturnResiduals: unprocessed elements", "bundle", rb, "count", len(unprocessedElements))
		count := stage.AddPending(unprocessedElements)
		em.addPending(count)
	}
	em.addRefreshes(singleSet(rb.StageID))
}

func (em *ElementManager) addRefreshes(stages set[string]) {
	em.refreshCond.L.Lock()
	defer em.refreshCond.L.Unlock()
	em.watermarkRefreshes.merge(stages)
	em.refreshCond.Broadcast()
}

func (em *ElementManager) addRefreshAndClearBundle(stageID, bundID string) {
	em.refreshCond.L.Lock()
	defer em.refreshCond.L.Unlock()
	delete(em.inprogressBundles, bundID)
	em.watermarkRefreshes.insert(stageID)
	em.refreshCond.Broadcast()
}

// refreshWatermarks incrementally refreshes the watermarks, and returns the set of stages where the
// the watermark may have advanced.
// Must be called while holding em.refreshCond.L
func (em *ElementManager) refreshWatermarks() set[string] {
	// Need to have at least one refresh signal.
	nextUpdates := set[string]{}
	refreshed := set[string]{}
	var i int
	for stageID := range em.watermarkRefreshes {
		// clear out old one.
		em.watermarkRefreshes.remove(stageID)
		ss := em.stages[stageID]
		refreshed.insert(stageID)

		refreshes := ss.updateWatermarks(em)
		nextUpdates.merge(refreshes)
		// cap refreshes incrementally.
		if i < 10 {
			i++
		} else {
			break
		}
	}
	em.watermarkRefreshes.merge(nextUpdates)
	return refreshed
}

type set[K comparable] map[K]struct{}

func (s set[K]) present(k K) bool {
	_, ok := s[k]
	return ok
}

func (s set[K]) remove(k K) {
	delete(s, k)
}

func (s set[K]) insert(k K) {
	s[k] = struct{}{}
}

func (s set[K]) merge(o set[K]) {
	for k := range o {
		s.insert(k)
	}
}

func singleSet[T comparable](v T) set[T] {
	return set[T]{v: struct{}{}}
}

// stageState is the internal watermark and input tracking for a stage.
type stageState struct {
	ID        string
	inputID   string   // PCollection ID of the parallel input
	outputIDs []string // PCollection IDs of outputs to update consumers.
	sides     []LinkID // PCollection IDs of side inputs that can block execution.

	// Special handling bits
	stateful  bool     // whether this stage uses state or timers, and needs keyed processing.
	aggregate bool     // whether this stage needs to block for aggregation.
	strat     winStrat // Windowing Strategy for aggregation fireings.

	mu                 sync.Mutex
	upstreamWatermarks sync.Map   // watermark set from inputPCollection's parent.
	input              mtime.Time // input watermark for the parallel input.
	output             mtime.Time // Output watermark for the whole stage
	estimatedOutput    mtime.Time // Estimated watermark output from DoFns

	pending    elementHeap                          // pending input elements for this stage that are to be processesd
	inprogress map[string]elements                  // inprogress elements by active bundles, keyed by bundle
	sideInputs map[LinkID]map[typex.Window][][]byte // side input data for this stage, from {tid, inputID} -> window

	// Fields for stateful stages which need to be per key.
	pendingByKeys          map[string]*dataAndTimers                        // pending input elements by Key, if stateful.
	inprogressKeys         set[string]                                      // all keys that are assigned to bundles.
	inprogressKeysByBundle map[string]set[string]                           // bundle to key assignments.
	state                  map[LinkID]map[typex.Window]map[string]StateData // state data for this stage, from {tid, stateID} -> window -> userKey

	// Accounting for handling watermark holds for timers.
	// We track the count of timers with the same hold, and clear it from
	// the map and heap when the count goes to zero.
	// This avoids scanning the heap to remove or access a hold for each element.
	watermarkHoldsCounts    map[mtime.Time]int
	watermarkHoldHeap       holdHeap
	inprogressHoldsByBundle map[string]map[mtime.Time]int // bundle to associated holds.
}

// timerKey uniquely identifies a given timer within the space of a user key.
type timerKey struct {
	family, tag string
	window      typex.Window
}

type timerTimes struct {
	firing, hold mtime.Time
}

// dataAndTimers represents all elements for a single user key and the latest
// eventTime for a given family and tag.
type dataAndTimers struct {
	elements elementHeap
	timers   map[timerKey]timerTimes
}

// holdHeap orders holds based on their timestamps
// so we can always find the minimum timestamp of pending holds.
type holdHeap []mtime.Time

func (h holdHeap) Len() int           { return len(h) }
func (h holdHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h holdHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *holdHeap) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(mtime.Time))
}

func (h *holdHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// makeStageState produces an initialized stageState.
func makeStageState(ID string, inputIDs, outputIDs []string, sides []LinkID) *stageState {
	ss := &stageState{
		ID:                   ID,
		outputIDs:            outputIDs,
		sides:                sides,
		strat:                defaultStrat{},
		state:                map[LinkID]map[typex.Window]map[string]StateData{},
		watermarkHoldsCounts: map[mtime.Time]int{},

		input:           mtime.MinTimestamp,
		output:          mtime.MinTimestamp,
		estimatedOutput: mtime.MinTimestamp,
	}

	// Initialize the upstream watermarks to minTime.
	for _, pcol := range inputIDs {
		ss.upstreamWatermarks.Store(pcol, mtime.MinTimestamp)
	}
	if len(inputIDs) == 1 {
		ss.inputID = inputIDs[0]
	}
	return ss
}

// AddPending adds elements to the pending heap.
func (ss *stageState) AddPending(newPending []element) int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.stateful {
		if ss.pendingByKeys == nil {
			ss.pendingByKeys = map[string]*dataAndTimers{}
		}
		count := 0
		for _, e := range newPending {
			count++
			if len(e.keyBytes) == 0 {
				panic(fmt.Sprintf("zero length key: %v %v", ss.ID, ss.inputID))
			}
			dnt, ok := ss.pendingByKeys[string(e.keyBytes)]
			if !ok {
				dnt = &dataAndTimers{
					timers: map[timerKey]timerTimes{},
				}
				ss.pendingByKeys[string(e.keyBytes)] = dnt
			}
			dnt.elements.Push(e)

			if e.IsTimer() {
				if lastSet, ok := dnt.timers[timerKey{family: e.family, tag: e.tag, window: e.window}]; ok {
					// existing timer!
					// don't increase the count this time, as "this" timer is already pending.
					count--
					// clear out the existing hold for accounting purposes.
					v := ss.watermarkHoldsCounts[lastSet.hold] - 1
					if v == 0 {
						delete(ss.watermarkHoldsCounts, lastSet.hold)
						for i, hold := range ss.watermarkHoldHeap {
							if hold == lastSet.hold {
								heap.Remove(&ss.watermarkHoldHeap, i)
								break
							}
						}
					} else {
						ss.watermarkHoldsCounts[lastSet.hold] = v
					}
				}
				// Update the last set time on the timer.
				dnt.timers[timerKey{family: e.family, tag: e.tag, window: e.window}] = timerTimes{firing: e.timestamp, hold: e.holdTimestamp}

				// Mark the hold in the heap.
				ss.watermarkHoldsCounts[e.holdTimestamp] = ss.watermarkHoldsCounts[e.holdTimestamp] + 1

				if len(ss.watermarkHoldsCounts) != len(ss.watermarkHoldHeap) {
					// The hold should not be in the heap, so we add it.
					heap.Push(&ss.watermarkHoldHeap, e.holdTimestamp)
				}
			}
		}
		return count
	}
	// Default path.
	ss.pending = append(ss.pending, newPending...)
	heap.Init(&ss.pending)
	return len(newPending)
}

// AddPendingSide adds elements to be consumed as side inputs.
func (ss *stageState) AddPendingSide(newPending []element, tID, inputID string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.sideInputs == nil {
		ss.sideInputs = map[LinkID]map[typex.Window][][]byte{}
	}
	key := LinkID{Transform: tID, Local: inputID}
	in, ok := ss.sideInputs[key]
	if !ok {
		in = map[typex.Window][][]byte{}
		ss.sideInputs[key] = in
	}
	for _, e := range newPending {
		in[e.window] = append(in[e.window], e.elmBytes)
	}
}

// GetSideData returns side input data for the provided transform+input pair, valid to the watermark.
func (ss *stageState) GetSideData(tID, inputID string, watermark mtime.Time) map[typex.Window][][]byte {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	d := ss.sideInputs[LinkID{Transform: tID, Local: inputID}]
	ret := map[typex.Window][][]byte{}
	for win, ds := range d {
		if win.MaxTimestamp() <= watermark {
			ret[win] = ds
		}
	}
	return ret
}

// GetSideData returns side input data for the provided stage+transform+input tuple, valid to the watermark.
func (em *ElementManager) GetSideData(sID, tID, inputID string, watermark mtime.Time) map[typex.Window][][]byte {
	return em.stages[sID].GetSideData(tID, inputID, watermark)
}

// updateUpstreamWatermark is for the parent of the input pcollection
// to call, to update downstream stages with it's current watermark.
// This avoids downstream stages inverting lock orderings from
// calling their parent stage to get their input pcollection's watermark.
func (ss *stageState) updateUpstreamWatermark(pcol string, upstream mtime.Time) {
	// A stage will only have a single upstream watermark, so
	// we simply set this.
	ss.upstreamWatermarks.Store(pcol, upstream)
}

// UpstreamWatermark gets the minimum value of all upstream watermarks.
func (ss *stageState) UpstreamWatermark() (string, mtime.Time) {
	upstream := mtime.MaxTimestamp
	var name string
	ss.upstreamWatermarks.Range(func(key, val any) bool {
		// Use <= to ensure if available we get a name.
		if val.(mtime.Time) <= upstream {
			upstream = val.(mtime.Time)
			name = key.(string)
		}
		return true
	})
	return name, upstream
}

// InputWatermark gets the current input watermark for the stage.
func (ss *stageState) InputWatermark() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.input
}

// OutputWatermark gets the current output watermark for the stage.
func (ss *stageState) OutputWatermark() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.output
}

// TODO: Move to better place for configuration
var (
	OneKeyPerBundle  bool // OneKeyPerBundle sets if a bundle is restricted to a single key.
	OneElementPerKey bool // OneElementPerKey sets if a key in a bundle is restricted to one element.
)

// startBundle initializes a bundle with elements if possible.
// A bundle only starts if there are elements at all, and if it's
// an aggregation stage, if the windowing stratgy allows it.
func (ss *stageState) startBundle(watermark mtime.Time, genBundID func() string) (string, bool, bool) {
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("generating bundle for stage %v at %v panicked\n%v", ss.ID, watermark, e))
		}
	}()
	ss.mu.Lock()
	defer ss.mu.Unlock()

	var toProcess, notYet []element
	for _, e := range ss.pending {
		if !ss.aggregate || ss.aggregate && ss.strat.EarliestCompletion(e.window) < watermark {
			toProcess = append(toProcess, e)
		} else {
			notYet = append(notYet, e)
		}
	}
	ss.pending = notYet
	heap.Init(&ss.pending)
	if ss.inprogressKeys == nil {
		ss.inprogressKeys = set[string]{}
	}
	minTs := mtime.MaxTimestamp
	// TODO: Allow configurable limit of keys per bundle, and elements per key to improve parallelism.
	// TODO: when we do, we need to ensure that the stage remains schedualable for bundle execution, for remaining pending elements and keys.
	// With the greedy approach, we don't need to since "new data" triggers a refresh, and so should completing processing of a bundle.
	newKeys := set[string]{}
	stillSchedulable := true

	holdsInBundle := map[mtime.Time]int{}

	// If timers are cleared, and we end up with nothing to process
	// we need to reschedule a watermark refresh, since those vestigial
	// timers might have held back the minimum pending watermark.
	timerCleared := false

keysPerBundle:
	for k, dnt := range ss.pendingByKeys {
		if ss.inprogressKeys.present(k) {
			continue
		}
		newKeys.insert(k)
		// Track the min-timestamp for later watermark handling.
		if dnt.elements[0].timestamp < minTs {
			minTs = dnt.elements[0].timestamp
		}

		// Can we pre-compute this bit when adding to pendingByKeys?
		// startBundle is in run in a single scheduling goroutine, so moving per-element code
		// to be computed by the bundle parallel goroutines will speed things up a touch.
		for dnt.elements.Len() > 0 {
			e := heap.Pop(&dnt.elements).(element)
			if e.IsTimer() {
				lastSet, ok := dnt.timers[timerKey{family: e.family, tag: e.tag, window: e.window}]
				if !ok {
					timerCleared = true
					continue // Timer has "fired" already, so this can be ignored.
				}
				if e.timestamp != lastSet.firing {
					timerCleared = true
					continue
				}
				holdsInBundle[e.holdTimestamp] = holdsInBundle[e.holdTimestamp] + 1
				// Clear the "fired" timer so subsequent matches can be ignored.
				delete(dnt.timers, timerKey{family: e.family, tag: e.tag, window: e.window})
			}
			toProcess = append(toProcess, e)
			if OneElementPerKey {
				break
			}
		}
		if dnt.elements.Len() == 0 {
			delete(ss.pendingByKeys, k)
		}
		if OneKeyPerBundle {
			break keysPerBundle
		}
	}
	if len(ss.pendingByKeys) == 0 && !timerCleared {
		// If we're out of data, and timers were not cleared then the watermark is are accurate.
		stillSchedulable = false
	}

	if len(toProcess) == 0 {
		// If we have nothing
		return "", false, stillSchedulable
	}

	if toProcess[0].timestamp < minTs {
		// Catch the ordinary case.
		minTs = toProcess[0].timestamp
	}

	es := elements{
		es:           toProcess,
		minTimestamp: minTs,
	}
	if ss.inprogress == nil {
		ss.inprogress = make(map[string]elements)
	}
	if ss.inprogressKeysByBundle == nil {
		ss.inprogressKeysByBundle = make(map[string]set[string])
	}
	if ss.inprogressHoldsByBundle == nil {
		ss.inprogressHoldsByBundle = make(map[string]map[mtime.Time]int)
	}
	bundID := genBundID()
	ss.inprogress[bundID] = es
	ss.inprogressKeysByBundle[bundID] = newKeys
	ss.inprogressKeys.merge(newKeys)
	ss.inprogressHoldsByBundle[bundID] = holdsInBundle
	return bundID, true, stillSchedulable
}

func (ss *stageState) splitBundle(rb RunBundle, firstResidual int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	es := ss.inprogress[rb.BundleID]
	slog.Debug("split elements", "bundle", rb, "elem count", len(es.es), "res", firstResidual)

	prim := es.es[:firstResidual]
	res := es.es[firstResidual:]

	es.es = prim
	ss.pending = append(ss.pending, res...)
	heap.Init(&ss.pending)
	ss.inprogress[rb.BundleID] = es
}

// minimumPendingTimestamp returns the minimum pending timestamp from all pending elements,
// including in progress ones.
//
// Assumes that the pending heap is initialized if it's not empty.
func (ss *stageState) minPendingTimestamp() mtime.Time {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.minPendingTimestampLocked()
}

// minPendingTimestampLocked must be called under the ss.mu Lock.
func (ss *stageState) minPendingTimestampLocked() mtime.Time {
	minPending := mtime.MaxTimestamp
	if len(ss.pending) != 0 {
		minPending = ss.pending[0].timestamp
	}
	if len(ss.pendingByKeys) != 0 {
		// TODO(lostluck): Can we figure out how to avoid checking every key on every watermark refresh?
		for _, dnt := range ss.pendingByKeys {
			minPending = mtime.Min(minPending, dnt.elements[0].timestamp)
		}
	}
	for _, es := range ss.inprogress {
		minPending = mtime.Min(minPending, es.minTimestamp)
	}
	return minPending
}

func (ss *stageState) String() string {
	pcol, up := ss.UpstreamWatermark()
	return fmt.Sprintf("[%v] IN: %v OUT: %v UP: %q %v, aggregation: %v", ss.ID, ss.input, ss.output, pcol, up, ss.aggregate)
}

// updateWatermarks performs the following operations:
//
// Watermark_In'  = MAX(Watermark_In, MIN(U(TS_Pending), U(Watermark_InputPCollection)))
// Watermark_Out' = MAX(Watermark_Out, MIN(Watermark_In', U(minWatermarkHold)))
// Watermark_PCollection = Watermark_Out_ProducingPTransform
func (ss *stageState) updateWatermarks(em *ElementManager) set[string] {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	minPending := ss.minPendingTimestampLocked()
	minWatermarkHold := mtime.MaxTimestamp
	if ss.watermarkHoldHeap.Len() > 0 {
		minWatermarkHold = ss.watermarkHoldHeap[0]
	}

	// PCollection watermarks are based on their parents's output watermark.
	_, newIn := ss.UpstreamWatermark()

	// Set the input watermark based on the minimum pending elements,
	// and the current input pcollection watermark.
	if minPending < newIn {
		newIn = minPending
	}

	// If bigger, advance the input watermark.
	if newIn > ss.input {
		ss.input = newIn
	}
	// The output starts with the new input as the basis.
	newOut := ss.input

	// If we're given an estimate, and it's further ahead, we use that instead.
	if ss.estimatedOutput > ss.output {
		newOut = ss.estimatedOutput
	}

	// We adjust based on the minimum state hold.
	// If we hold it, mark this stage as refreshable?
	if minWatermarkHold < newOut {
		newOut = minWatermarkHold
	}
	refreshes := set[string]{}
	// If bigger, advance the output watermark
	if newOut > ss.output {
		ss.output = newOut
		for _, outputCol := range ss.outputIDs {
			consumers := em.consumers[outputCol]

			for _, sID := range consumers {
				em.stages[sID].updateUpstreamWatermark(outputCol, ss.output)
				refreshes.insert(sID)
			}
			// Inform side input consumers, but don't update the upstream watermark.
			for _, sID := range em.sideConsumers[outputCol] {
				refreshes.insert(sID.Global)
			}
		}
		// Garbage collect state, timers and side inputs, for all windows
		// that are before the new output watermark.
		// They'll never be read in again.
		for _, wins := range ss.sideInputs {
			for win := range wins {
				// Clear out anything we've already used.
				if win.MaxTimestamp() < newOut {
					delete(wins, win)
				}
			}
		}
		for _, wins := range ss.state {
			for win := range wins {
				// Clear out anything we've already used.
				if win.MaxTimestamp() < newOut {
					delete(wins, win)
				}
			}
		}
	}
	return refreshes
}

// bundleReady returns the maximum allowed watermark for this stage, and whether
// it's permitted to execute by side inputs.
func (ss *stageState) bundleReady(em *ElementManager) (mtime.Time, bool) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	// If the upstream watermark and the input watermark are the same,
	// then we can't yet process this stage.
	inputW := ss.input
	_, upstreamW := ss.UpstreamWatermark()
	if inputW == upstreamW {
		slog.Debug("bundleReady: insufficient upstream watermark",
			slog.String("stage", ss.ID),
			slog.Group("watermark",
				slog.Any("upstream", upstreamW),
				slog.Any("input", inputW)))
		return mtime.MinTimestamp, false
	}
	ready := true
	for _, side := range ss.sides {
		pID, ok := em.pcolParents[side.Global]
		if !ok {
			panic(fmt.Sprintf("stage[%v] no parent ID for side input %v", ss.ID, side))
		}
		parent, ok := em.stages[pID]
		if !ok {
			panic(fmt.Sprintf("stage[%v] no parent for side input %v, with parent ID %v", ss.ID, side, pID))
		}
		ow := parent.OutputWatermark()
		if upstreamW > ow {
			ready = false
		}
	}
	return upstreamW, ready
}
