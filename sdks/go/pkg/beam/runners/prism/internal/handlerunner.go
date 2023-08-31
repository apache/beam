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
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/engine"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/urns"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism/internal/worker"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// This file retains the logic for the pardo handler

// RunnerCharacteristic holds the configuration for Runner based transforms,
// such as GBKs, Flattens.
type RunnerCharacteristic struct {
	SDKFlatten   bool // Sets whether we should force an SDK side flatten.
	SDKGBK       bool // Sets whether the GBK should be handled by the SDK, if possible by the SDK.
	SDKReshuffle bool // Sets whether we should use the SDK backup implementation to handle a Reshuffle.
}

func Runner(config any) *runner {
	return &runner{config: config.(RunnerCharacteristic)}
}

// runner represents an instance of the runner transform handler.
type runner struct {
	config RunnerCharacteristic
}

// ConfigURN returns the name for combine in the configuration file.
func (*runner) ConfigURN() string {
	return "runner"
}

func (*runner) ConfigCharacteristic() reflect.Type {
	return reflect.TypeOf((*RunnerCharacteristic)(nil)).Elem()
}

var _ transformPreparer = (*runner)(nil)

func (*runner) PrepareUrns() []string {
	return []string{urns.TransformReshuffle}
}

// PrepareTransform handles special processing with respect runner transforms, like reshuffle.
func (h *runner) PrepareTransform(tid string, t *pipepb.PTransform, comps *pipepb.Components) (*pipepb.Components, []string) {
	// TODO: Implement the windowing strategy the "backup" transforms used for Reshuffle.
	// TODO: Implement a fusion break for reshuffles.

	if h.config.SDKReshuffle {
		panic("SDK side reshuffle not yet supported")
	}

	// A Reshuffle, in principle, is a no-op on the pipeline structure, WRT correctness.
	// It could however affect performance, so it exists to tell the runner that this
	// point in the pipeline needs a fusion break, to enable the pipeline to change it's
	// degree of parallelism.
	//
	// The change of parallelism goes both ways. It could allow for larger batch sizes
	// enable smaller batch sizes downstream if it is infact paralleizable.
	//
	// But for a single transform node per stage runner, we can elide it entirely,
	// since the input collection and output collection types match.

	// Get the input and output PCollections, there should only be 1 each.
	if len(t.GetInputs()) != 1 {
		panic("Expected single input PCollection in reshuffle: " + prototext.Format(t))
	}
	if len(t.GetOutputs()) != 1 {
		panic("Expected single output PCollection in reshuffle: " + prototext.Format(t))
	}

	inColID := getOnlyValue(t.GetInputs())
	outColID := getOnlyValue(t.GetOutputs())

	// We need to find all Transforms that consume the output collection and
	// replace them so they consume the input PCollection directly.

	// We need to remove the consumers of the output PCollection.
	toRemove := []string{}

	for _, t := range comps.GetTransforms() {
		for li, gi := range t.GetInputs() {
			if gi == outColID {
				// The whole s
				t.GetInputs()[li] = inColID
			}
		}
	}

	// And all the sub transforms.
	toRemove = append(toRemove, t.GetSubtransforms()...)

	// Return the new components which is the transforms consumer
	return nil, toRemove
}

var _ transformExecuter = (*runner)(nil)

func (*runner) ExecuteUrns() []string {
	return []string{urns.TransformFlatten, urns.TransformGBK, urns.TransformReshuffle}
}

// ExecuteWith returns what environment the transform should execute in.
func (h *runner) ExecuteWith(t *pipepb.PTransform) string {
	urn := t.GetSpec().GetUrn()
	if urn == urns.TransformFlatten && !h.config.SDKFlatten {
		return ""
	}
	if urn == urns.TransformGBK && !h.config.SDKGBK {
		return ""
	}
	return t.GetEnvironmentId()
}

// ExecuteTransform handles special processing with respect to runner specific transforms
func (h *runner) ExecuteTransform(stageID, tid string, t *pipepb.PTransform, comps *pipepb.Components, watermark mtime.Time, inputData [][]byte) *worker.B {
	urn := t.GetSpec().GetUrn()
	var data [][]byte
	var onlyOut string
	for _, out := range t.GetOutputs() {
		onlyOut = out
	}

	switch urn {
	case urns.TransformFlatten:
		// Already done and collated.
		data = inputData

	case urns.TransformGBK:
		ws := windowingStrategy(comps, tid)
		kvc := onlyInputCoderForTransform(comps, tid)

		coders := map[string]*pipepb.Coder{}

		// TODO assert this is a KV. It's probably fine, but we should fail anyway.
		wcID, err := lpUnknownCoders(ws.GetWindowCoderId(), coders, comps.GetCoders())
		if err != nil {
			panic(fmt.Errorf("ExecuteTransform[GBK] stage %v, transform %q %v: couldn't process window coder:\n%w", stageID, tid, prototext.Format(t), err))
		}
		kcID, err := lpUnknownCoders(kvc.GetComponentCoderIds()[0], coders, comps.GetCoders())
		if err != nil {
			panic(fmt.Errorf("ExecuteTransform[GBK] stage %v, transform %q %v: couldn't process key coder:\n%w", stageID, tid, prototext.Format(t), err))
		}
		ecID, err := lpUnknownCoders(kvc.GetComponentCoderIds()[1], coders, comps.GetCoders())
		if err != nil {
			panic(fmt.Errorf("ExecuteTransform[GBK] stage %v, transform %q %v: couldn't process value coder:\n%w", stageID, tid, prototext.Format(t), err))
		}
		reconcileCoders(coders, comps.GetCoders())

		wc := coders[wcID]
		kc := coders[kcID]
		ec := coders[ecID]

		data = append(data, gbkBytes(ws, wc, kc, ec, inputData, coders, watermark))
		if len(data[0]) == 0 {
			panic("no data for GBK")
		}
	default:
		panic(fmt.Sprintf("unimplemented runner transform[%v]", urn))
	}

	// To avoid conflicts with these single transform
	// bundles, we suffix the transform IDs.
	var localID string
	for key := range t.GetOutputs() {
		localID = key
	}

	if localID == "" {
		panic(fmt.Sprintf("bad transform: %v", prototext.Format(t)))
	}
	output := engine.TentativeData{}
	for _, d := range data {
		output.WriteData(onlyOut, d)
	}

	dataID := tid + "_" + localID // The ID from which the consumer will read from.
	b := &worker.B{
		InputTransformID: dataID,
		SinkToPCollection: map[string]string{
			dataID: onlyOut,
		},
		OutputData: output,
	}
	return b
}

// windowingStrategy sources the transform's windowing strategy from a single parallel input.
func windowingStrategy(comps *pipepb.Components, tid string) *pipepb.WindowingStrategy {
	t := comps.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comps.GetPcollections()[inputPColID]
	return comps.GetWindowingStrategies()[pcol.GetWindowingStrategyId()]
}

// gbkBytes re-encodes gbk inputs in a gbk result.
func gbkBytes(ws *pipepb.WindowingStrategy, wc, kc, vc *pipepb.Coder, toAggregate [][]byte, coders map[string]*pipepb.Coder, watermark mtime.Time) []byte {
	var outputTime func(typex.Window, mtime.Time) mtime.Time
	switch ws.GetOutputTime() {
	case pipepb.OutputTime_END_OF_WINDOW:
		outputTime = func(w typex.Window, et mtime.Time) mtime.Time {
			return w.MaxTimestamp()
		}
	default:
		// TODO need to correct session logic if output time is different.
		panic(fmt.Sprintf("unsupported OutputTime behavior: %v", ws.GetOutputTime()))
	}
	wDec, wEnc := makeWindowCoders(wc)

	type keyTime struct {
		key    []byte
		w      typex.Window
		time   mtime.Time
		values [][]byte
	}
	// Map windows to a map of keys to a map of keys to time.
	// We ultimately emit the window, the key, the time, and the iterable of elements,
	// all contained in the final value.
	windows := map[typex.Window]map[string]keyTime{}

	kd := pullDecoder(kc, coders)
	vd := pullDecoder(vc, coders)

	// Right, need to get the key coder, and the element coder.
	// Cus I'll need to pull out anything the runner knows how to deal with.
	// And repeat.
	for _, data := range toAggregate {
		// Parse out each element's data, and repeat.
		buf := bytes.NewBuffer(data)
		for {
			ws, tm, _, err := exec.DecodeWindowedValueHeader(wDec, buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(fmt.Sprintf("can't decode windowed value header with %v: %v", wc, err))
			}

			keyByt := kd(buf)
			key := string(keyByt)
			value := vd(buf)
			for _, w := range ws {
				ft := outputTime(w, tm)
				wk, ok := windows[w]
				if !ok {
					wk = make(map[string]keyTime)
					windows[w] = wk
				}
				kt := wk[key]
				kt.time = ft
				kt.key = keyByt
				kt.w = w
				kt.values = append(kt.values, value)
				wk[key] = kt
			}
		}
	}

	// If the strategy is session windows, then we need to get all the windows, sort them
	// and see which ones need to be merged together.
	if ws.GetWindowFn().GetUrn() == urns.WindowFnSession {
		slog.Debug("sorting by session window")
		session := &pipepb.SessionWindowsPayload{}
		if err := (proto.UnmarshalOptions{}).Unmarshal(ws.GetWindowFn().GetPayload(), session); err != nil {
			panic("unable to decode SessionWindowsPayload")
		}
		gapSize := mtime.Time(session.GetGapSize().AsDuration())

		ordered := make([]window.IntervalWindow, 0, len(windows))
		for k := range windows {
			ordered = append(ordered, k.(window.IntervalWindow))
		}
		// Use a decreasing sort (latest to earliest) so we can correct
		// the output timestamp to the new end of window immeadiately.
		// TODO need to correct this if output time is different.
		sort.Slice(ordered, func(i, j int) bool {
			return ordered[i].MaxTimestamp() > ordered[j].MaxTimestamp()
		})

		cur := ordered[0]
		sessionData := windows[cur]
		for _, iw := range ordered[1:] {
			// If they overlap, then we merge the data.
			if iw.End+gapSize < cur.Start {
				// Start a new session.
				windows[cur] = sessionData
				cur = iw
				sessionData = windows[iw]
				continue
			}
			// Extend the session
			cur.Start = iw.Start
			toMerge := windows[iw]
			delete(windows, iw)
			for k, kt := range toMerge {
				skt := sessionData[k]
				skt.key = kt.key
				skt.w = cur
				skt.values = append(skt.values, kt.values...)
				sessionData[k] = skt
			}
		}
	}
	// Everything's aggregated!
	// Time to turn things into a windowed KV<K, Iterable<V>>

	var buf bytes.Buffer
	for _, w := range windows {
		for _, kt := range w {
			exec.EncodeWindowedValueHeader(
				wEnc,
				[]typex.Window{kt.w},
				kt.time,
				typex.NoFiringPane(),
				&buf,
			)
			buf.Write(kt.key)
			coder.EncodeInt32(int32(len(kt.values)), &buf)
			for _, value := range kt.values {
				buf.Write(value)
			}
		}
	}
	return buf.Bytes()
}

func onlyInputCoderForTransform(comps *pipepb.Components, tid string) *pipepb.Coder {
	t := comps.GetTransforms()[tid]
	var inputPColID string
	for _, pcolID := range t.GetInputs() {
		inputPColID = pcolID
	}
	pcol := comps.GetPcollections()[inputPColID]
	return comps.GetCoders()[pcol.GetCoderId()]
}
