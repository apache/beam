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

package dataflowlib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"path"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/pipelinex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/stringx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pubsub_v1 "github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio/v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
	df "google.golang.org/api/dataflow/v1b3"
)

const (
	impulseKind    = "CreateCollection"
	parDoKind      = "ParallelDo"
	combineKind    = "CombineValues"
	flattenKind    = "Flatten"
	gbkKind        = "GroupByKey"
	windowIntoKind = "Bucket"

	sideInputKind = "CollectionToSingleton"

	// Support for Dataflow native I/O, such as PubSub.
	readKind  = "ParallelRead"
	writeKind = "ParallelWrite"
)

// translate translates a pipeline into a sequence of Dataflow steps. The step
// representation and its semantics are complex. In particular, the service
// optimizes the steps (step fusing, etc.) and may move steps around. Our
// decorations of the steps must thus be robust against such changes, so that
// they can be properly decoded in the harness. There are multiple quirks and
// requirements of specific semi-opaque formats, such as base64 encoded blobs.
//
// Moreover, the harness sees pieces of the translated steps only -- not the
// full graph. Special steps are also inserted around GBK, for example, which
// makes the placement of the decoration somewhat tricky. The harness will
// also never see steps that the service executes directly, notably GBK/CoGBK.
func translate(p *pipepb.Pipeline) ([]*df.Step, error) {
	// NOTE: Dataflow apparently assumes that the steps are in topological order.
	// Otherwise, it fails with "Output out for step  was not found.". We assume
	// the pipeline has been normalized and each subtransform list is in such order.

	x := newTranslator(p.GetComponents())
	return x.translateTransforms("", p.GetRootTransformIds())
}

type translator struct {
	comp          *pipepb.Components
	pcollections  map[string]*outputReference
	coders        *graphx.CoderUnmarshaller
	bogusCoderRef *graphx.CoderRef
}

func newTranslator(comp *pipepb.Components) *translator {
	bytesCoderRef, _ := graphx.EncodeCoderRef(coder.NewW(coder.NewBytes(), coder.NewGlobalWindow()))

	return &translator{
		comp:          comp,
		pcollections:  makeOutputReferences(comp.GetTransforms()),
		coders:        graphx.NewCoderUnmarshaller(comp.GetCoders()),
		bogusCoderRef: bytesCoderRef,
	}
}

func (x *translator) translateTransforms(trunk string, ids []string) ([]*df.Step, error) {
	var steps []*df.Step
	for _, id := range ids {
		sub, err := x.translateTransform(trunk, id)
		if err != nil {
			return nil, err
		}
		steps = append(steps, sub...)
	}
	return steps, nil
}

func (x *translator) translateTransform(trunk string, id string) ([]*df.Step, error) {
	t := x.comp.Transforms[id]

	prop := properties{
		UserName:   userName(trunk, t.UniqueName),
		OutputInfo: x.translateOutputs(t.Outputs),
	}

	urn := t.GetSpec().GetUrn()
	switch urn {
	case graphx.URNImpulse:
		// NOTE: The impulse []data value is encoded in a special way as a
		// URL Query-escaped windowed _unnested_ value. It is read back in
		// a nested context at runtime.
		var buf bytes.Buffer
		if err := exec.EncodeWindowedValueHeader(exec.MakeWindowEncoder(coder.NewGlobalWindow()), window.SingleGlobalWindow, mtime.ZeroTimestamp, &buf); err != nil {
			return nil, err
		}
		value := string(append(buf.Bytes(), t.GetSpec().Payload...))
		// log.Printf("Impulse data: %v", url.QueryEscape(value))

		prop.Element = []string{url.QueryEscape(value)}
		return []*df.Step{x.newStep(id, impulseKind, prop)}, nil

	case graphx.URNParDo:
		var payload pipepb.ParDoPayload
		if err := proto.Unmarshal(t.Spec.Payload, &payload); err != nil {
			return nil, errors.Wrapf(err, "invalid ParDo payload for %v", t)
		}

		var steps []*df.Step
		rem := reflectx.ShallowClone(t.Inputs).(map[string]string)

		prop.NonParallelInputs = make(map[string]*outputReference)
		for key, sideInput := range payload.SideInputs {
			// Side input require an additional conversion step, which must
			// be before the present one.
			delete(rem, key)

			pcol := x.comp.Pcollections[t.Inputs[key]]
			ref := x.pcollections[t.Inputs[key]]
			c := x.translateCoder(pcol, pcol.CoderId)

			var outputInfo output
			outputInfo = output{
				UserName:   "i0",
				OutputName: "i0",
				Encoding:   graphx.WrapIterable(c),
			}
			if graphx.URNMultimapSideInput == sideInput.GetAccessPattern().GetUrn() {
				outputInfo.UseIndexedFormat = true
			}

			side := &df.Step{
				Name: fmt.Sprintf("view%v_%v", id, key),
				Kind: sideInputKind,
				Properties: newMsg(properties{
					ParallelInput: ref,
					OutputInfo: []output{
						outputInfo,
					},
					UserName: userName(trunk, fmt.Sprintf("AsView%v_%v", id, key)),
				}),
			}
			steps = append(steps, side)

			prop.NonParallelInputs[key] = newOutputReference(side.Name, "i0")
		}

		rcid := payload.GetRestrictionCoderId()
		if rcid != "" {
			rc, err := x.coders.Coder(rcid)
			if err != nil {
				return nil, err
			}
			enc, err := graphx.EncodeCoderRef(rc)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid splittable ParDoPayload, couldn't encode Restriction Coder %v", t)
			}
			prop.RestrictionEncoder = enc
		}

		in := stringx.SingleValue(rem)

		prop.ParallelInput = x.pcollections[in]
		prop.SerializedFn = id // == reference into the proto pipeline
		return append(steps, x.newStep(id, parDoKind, prop)), nil
	case graphx.URNCombinePerKey:
		// Dataflow uses a GBK followed by a CombineValues to determine when it can lift.
		// To achieve this, we use the combine composite's subtransforms, and modify the
		// Combine ParDo with the CombineValues kind, set its SerializedFn to map to the
		// composite payload, and the accumulator coding.
		if len(t.Subtransforms) != 2 {
			return nil, errors.Errorf("invalid CombinePerKey, expected 2 subtransforms but got %d in %v", len(t.Subtransforms), t)
		}
		steps, err := x.translateTransforms(fmt.Sprintf("%v%v/", trunk, path.Base(t.UniqueName)), t.Subtransforms)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid CombinePerKey, couldn't extract GBK from %v", t)
		}
		var payload pipepb.CombinePayload
		if err := proto.Unmarshal(t.Spec.Payload, &payload); err != nil {
			return nil, errors.Wrapf(err, "invalid Combine payload for %v", t)
		}

		c, err := x.coders.Coder(payload.AccumulatorCoderId)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid Combine payload , missing Accumulator Coder %v", t)
		}
		enc, err := graphx.EncodeCoderRef(c)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid Combine payload, couldn't encode Accumulator Coder %v", t)
		}
		json.Unmarshal([]byte(steps[1].Properties), &prop)
		prop.Encoding = enc
		prop.SerializedFn = id
		steps[1].Kind = combineKind
		steps[1].Properties = newMsg(prop)
		return steps, nil

	case graphx.URNReshuffle:
		return x.translateTransforms(fmt.Sprintf("%v%v/", trunk, path.Base(t.UniqueName)), t.Subtransforms)

	case graphx.URNFlatten:
		for _, in := range t.Inputs {
			prop.Inputs = append(prop.Inputs, x.pcollections[in])
		}
		return []*df.Step{x.newStep(id, flattenKind, prop)}, nil

	case graphx.URNGBK:
		in := stringx.SingleValue(t.Inputs)

		prop.ParallelInput = x.pcollections[in]
		prop.SerializedFn = encodeSerializedFn(x.extractWindowingStrategy(in))
		return []*df.Step{x.newStep(id, gbkKind, prop)}, nil

	case graphx.URNWindow:
		in := stringx.SingleValue(t.Inputs)
		out := stringx.SingleValue(t.Outputs)

		prop.ParallelInput = x.pcollections[in]
		prop.SerializedFn = encodeSerializedFn(x.extractWindowingStrategy(out))
		return []*df.Step{x.newStep(id, windowIntoKind, prop)}, nil

	case pubsub_v1.PubSubPayloadURN:
		// Translate to native handling of PubSub I/O.

		var msg pubsub_v1.PubSubPayload
		if err := proto.Unmarshal(t.Spec.Payload, &msg); err != nil {
			return nil, errors.Wrap(err, "bad pubsub payload")
		}

		prop.Format = "pubsub"
		prop.PubSubTopic = msg.GetTopic()
		prop.PubSubSubscription = msg.GetSubscription()
		prop.PubSubIDLabel = msg.GetIdAttribute()
		prop.PubSubTimestampLabel = msg.GetTimestampAttribute()
		prop.PubSubWithAttributes = msg.GetWithAttributes()

		if prop.PubSubSubscription != "" {
			prop.PubSubTopic = ""
		}

		switch msg.Op {
		case pubsub_v1.PubSubPayload_READ:
			return []*df.Step{x.newStep(id, readKind, prop)}, nil

		case pubsub_v1.PubSubPayload_WRITE:
			in := stringx.SingleValue(t.Inputs)

			prop.ParallelInput = x.pcollections[in]
			prop.Encoding = x.wrapCoder(x.comp.Pcollections[in], coder.NewBytes())
			return []*df.Step{x.newStep(id, writeKind, prop)}, nil

		default:
			return nil, errors.Errorf("bad pubsub op: %v", msg.Op)
		}

	default:
		if len(t.Subtransforms) > 0 {
			return x.translateTransforms(fmt.Sprintf("%v%v/", trunk, path.Base(t.UniqueName)), t.Subtransforms)
		}

		return nil, errors.Errorf("unexpected primitive urn: %v", t)
	}
}

func (x *translator) newStep(id, kind string, prop properties) *df.Step {
	step := &df.Step{
		Name:       id,
		Kind:       kind,
		Properties: newMsg(prop),
	}
	if prop.PubSubWithAttributes {
		// Hack to add a empty-value property for PubSub IO. This
		// will make PubSub send the entire message, not just
		// the payload.
		prop.PubSubWithAttributes = false
		step.Properties = newMsg(propertiesWithPubSubMessage{properties: prop})
	}
	return step
}

func (x *translator) translateOutputs(outputs map[string]string) []output {
	var ret []output
	for _, out := range outputs {
		pcol := x.comp.Pcollections[out]
		ref := x.pcollections[out]

		info := output{
			UserName:   ref.OutputName,
			OutputName: ref.OutputName,
			Encoding:   x.translateCoder(pcol, pcol.CoderId),
		}
		ret = append(ret, info)
	}
	if len(ret) == 0 {
		// Dataflow seems to require at least one output. We insert
		// a bogus one (named "bogus") and remove it in the harness.

		ret = []output{{
			UserName:   "bogus",
			OutputName: "bogus",
			Encoding:   x.bogusCoderRef,
		}}
	}
	return ret
}

func (x *translator) translateCoder(pcol *pipepb.PCollection, id string) *graphx.CoderRef {
	c, err := x.coders.Coder(id)
	if err != nil {
		panic(err)
	}
	return x.wrapCoder(pcol, c)
}

func (x *translator) wrapCoder(pcol *pipepb.PCollection, c *coder.Coder) *graphx.CoderRef {
	// TODO(herohde) 3/16/2018: ensure windowed values for Dataflow

	ws := x.comp.WindowingStrategies[pcol.WindowingStrategyId]
	wc, err := x.coders.WindowCoder(ws.WindowCoderId)
	if err != nil {
		panic(errors.Wrapf(err, "failed to decode window coder %v for windowing strategy %v", ws.WindowCoderId, pcol.WindowingStrategyId))
	}
	ret, err := graphx.EncodeCoderRef(coder.NewW(c, wc))
	if err != nil {
		panic(errors.Wrapf(err, "failed to wrap coder %v for windowing strategy %v", c, pcol.WindowingStrategyId))
	}
	return ret
}

// extractWindowingStrategy returns a self-contained windowing strategy from
// the given pcollection id.
func (x *translator) extractWindowingStrategy(pid string) *pipepb.MessageWithComponents {
	ws := x.comp.WindowingStrategies[x.comp.Pcollections[pid].WindowingStrategyId]

	msg := &pipepb.MessageWithComponents{
		Components: &pipepb.Components{
			Coders: pipelinex.TrimCoders(x.comp.Coders, ws.WindowCoderId),
		},
		Root: &pipepb.MessageWithComponents_WindowingStrategy{
			WindowingStrategy: ws,
		},
	}
	return msg
}

// makeOutputReferences builds a map from PCollection id to the Dataflow representation.
// Each output is named after the generating transform.
func makeOutputReferences(xforms map[string]*pipepb.PTransform) map[string]*outputReference {
	ret := make(map[string]*outputReference)
	for id, t := range xforms {
		if len(t.Subtransforms) > 0 {
			continue // ignore composites
		}
		for name, out := range t.Outputs {
			ret[out] = newOutputReference(id, name)
		}
	}
	return ret
}

// userName computes a Dataflow composite name understood by the Dataflow UI,
// determined by the scope nesting. Dataflow simply uses "/" to separate
// composite transforms, so we must remove them from the otherwise qualified
// package names of DoFns, etc. Assumes trunk ends in / or is empty.
func userName(trunk, name string) string {
	return fmt.Sprintf("%v%v", trunk, path.Base(name))
}

func encodeSerializedFn(in proto.Message) string {
	// The Beam Runner API uses percent-encoding for serialized fn messages.
	// See: https://en.wikipedia.org/wiki/Percent-encoding

	data := protox.MustEncode(in)
	return url.PathEscape(string(data))
}
