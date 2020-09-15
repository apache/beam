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

package graphx

import (
	"encoding/json"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	v1pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

const (
	// Model constants

	urnBytesCoder               = "beam:coder:bytes:v1"
	urnBoolCoder                = "beam:coder:bool:v1"
	urnVarIntCoder              = "beam:coder:varint:v1"
	urnDoubleCoder              = "beam:coder:double:v1"
	urnStringCoder              = "beam:coder:string_utf8:v1"
	urnLengthPrefixCoder        = "beam:coder:length_prefix:v1"
	urnKVCoder                  = "beam:coder:kv:v1"
	urnIterableCoder            = "beam:coder:iterable:v1"
	urnStateBackedIterableCoder = "beam:coder:state_backed_iterable:v1"
	urnWindowedValueCoder       = "beam:coder:windowed_value:v1"

	urnGlobalWindow   = "beam:coder:global_window:v1"
	urnIntervalWindow = "beam:coder:interval_window:v1"

	// SDK constants

	urnCustomCoder = "beam:go:coder:custom:v1"
	urnCoGBKList   = "beam:go:coder:cogbklist:v1" // CoGBK representation. Not a coder.
)

func knownStandardCoders() []string {
	return []string{
		urnBytesCoder,
		urnBoolCoder,
		urnVarIntCoder,
		urnDoubleCoder,
		urnStringCoder,
		urnLengthPrefixCoder,
		urnKVCoder,
		urnIterableCoder,
		urnStateBackedIterableCoder,
		urnWindowedValueCoder,
		urnGlobalWindow,
		urnIntervalWindow,
	}
}

// MarshalCoders marshals a list of coders into model coders.
func MarshalCoders(coders []*coder.Coder) ([]string, map[string]*pipepb.Coder) {
	b := NewCoderMarshaller()
	ids := b.AddMulti(coders)
	return ids, b.Build()
}

// UnmarshalCoders unmarshals coders.
func UnmarshalCoders(ids []string, m map[string]*pipepb.Coder) ([]*coder.Coder, error) {
	b := NewCoderUnmarshaller(m)

	var coders []*coder.Coder
	for _, id := range ids {
		c, err := b.Coder(id)
		if err != nil {
			return nil, err
		}
		coders = append(coders, c)
	}
	return coders, nil
}

// CoderUnmarshaller is an incremental unmarshaller of model coders. Identical
// coders are shared.
type CoderUnmarshaller struct {
	models map[string]*pipepb.Coder

	coders       map[string]*coder.Coder
	windowCoders map[string]*coder.WindowCoder
}

// NewCoderUnmarshaller returns a new CoderUnmarshaller.
func NewCoderUnmarshaller(m map[string]*pipepb.Coder) *CoderUnmarshaller {
	return &CoderUnmarshaller{
		models:       m,
		coders:       make(map[string]*coder.Coder),
		windowCoders: make(map[string]*coder.WindowCoder),
	}
}

// Coders unmarshals a list of coder ids.
func (b *CoderUnmarshaller) Coders(ids []string) ([]*coder.Coder, error) {
	coders := make([]*coder.Coder, len(ids))
	for i, id := range ids {
		c, err := b.Coder(id)
		if err != nil {
			return nil, err
		}
		coders[i] = c
	}
	return coders, nil
}

// Coder unmarshals a coder with the given id.
func (b *CoderUnmarshaller) Coder(id string) (*coder.Coder, error) {
	if c, exists := b.coders[id]; exists {
		return c, nil
	}
	c, ok := b.models[id]
	if !ok {
		err := errors.Errorf("coder with id %v not found", id)
		return nil, errors.WithContextf(err, "unmarshalling coder %v", id)
	}

	ret, err := b.makeCoder(c)
	if err != nil {
		return nil, errors.WithContextf(err, "unmarshalling coder %v", id)
	}
	ret.ID = id

	b.coders[id] = ret
	return ret, nil
}

// WindowCoder unmarshals a window coder with the given id.
func (b *CoderUnmarshaller) WindowCoder(id string) (*coder.WindowCoder, error) {
	if w, exists := b.windowCoders[id]; exists {
		return w, nil
	}

	c, err := b.peek(id)
	if err != nil {
		return nil, err
	}

	w := urnToWindowCoder(c.GetSpec().GetUrn())
	b.windowCoders[id] = w
	return w, nil
}

func urnToWindowCoder(urn string) *coder.WindowCoder {
	switch urn {
	case urnGlobalWindow:
		return coder.NewGlobalWindow()
	case urnIntervalWindow:
		return coder.NewIntervalWindow()
	default:
		panic(fmt.Sprintf("Failed to translate URN to window coder, unexpected URN: %v", urn))
	}
}

func (b *CoderUnmarshaller) makeCoder(c *pipepb.Coder) (*coder.Coder, error) {
	urn := c.GetSpec().GetUrn()
	components := c.GetComponentCoderIds()

	switch urn {
	case urnBytesCoder:
		return coder.NewBytes(), nil

	case urnBoolCoder:
		return coder.NewBool(), nil

	case urnVarIntCoder:
		return coder.NewVarInt(), nil

	case urnDoubleCoder:
		return coder.NewDouble(), nil

	case urnStringCoder:
		return coder.NewString(), nil

	case urnKVCoder:
		if len(components) != 2 {
			return nil, errors.Errorf("could not unmarshal KV coder from %v, want exactly 2 components but have %d", c, len(components))
		}

		key, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}

		id := components[1]
		kind := coder.KV
		root := typex.KVType

		elm, err := b.peek(id)
		if err != nil {
			return nil, err
		}

		switch elm.GetSpec().GetUrn() {
		case urnIterableCoder, urnStateBackedIterableCoder:
			id = elm.GetComponentCoderIds()[0]
			kind = coder.CoGBK
			root = typex.CoGBKType

			// TODO(BEAM-490): If CoGBK with > 1 input, handle as special GBK. We expect
			// it to be encoded as CoGBK<K,LP<CoGBKList<V,W,..>>>. Remove this handling once
			// CoGBK has a first-class representation.

			if ids, ok := b.isCoGBKList(id); ok {
				// CoGBK<K,V,W,..>

				values, err := b.Coders(ids)
				if err != nil {
					return nil, err
				}

				t := typex.New(root, append([]typex.FullType{key.T}, coder.Types(values)...)...)
				return &coder.Coder{Kind: kind, T: t, Components: append([]*coder.Coder{key}, values...)}, nil
			}
		}

		value, err := b.Coder(id)
		if err != nil {
			return nil, err
		}

		t := typex.New(root, key.T, value.T)
		return &coder.Coder{Kind: kind, T: t, Components: []*coder.Coder{key, value}}, nil

	case urnLengthPrefixCoder:
		if len(components) != 1 {
			return nil, errors.Errorf("could not unmarshal length prefix coder from %v, want a single sub component but have %d", c, len(components))
		}

		sub, err := b.peek(components[0])
		if err != nil {
			return nil, err
		}

		// No payload means this coder was length prefixed by the runner
		// but is likely self describing - AKA a beam coder.
		if len(sub.GetSpec().GetPayload()) == 0 {
			return b.makeCoder(sub)
		}
		// TODO(lostluck) 2018/10/17: Make this strict again, once dataflow can use
		// the portable pipeline model directly (BEAM-2885)
		if sub.GetSpec().GetUrn() != "" && sub.GetSpec().GetUrn() != urnCustomCoder {
			// TODO(herohde) 11/17/2017: revisit this restriction
			return nil, errors.Errorf("could not unmarshal length prefix coder from %v, want a custom coder as a sub component but got %v", c, sub)
		}

		var ref v1pb.CustomCoder
		if err := protox.DecodeBase64(string(sub.GetSpec().GetPayload()), &ref); err != nil {
			return nil, err
		}
		custom, err := decodeCustomCoder(&ref)
		if err != nil {
			return nil, err
		}
		custom.ID = components[0]
		t := typex.New(custom.Type)
		return &coder.Coder{Kind: coder.Custom, T: t, Custom: custom}, nil

	case urnWindowedValueCoder:
		if len(components) != 2 {
			return nil, errors.Errorf("could not unmarshal windowed value coder from %v, expected two components but got %d", c, len(components))
		}

		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		w, err := b.WindowCoder(components[1])
		if err != nil {
			return nil, err
		}
		t := typex.New(typex.WindowedValueType, elm.T)
		return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: []*coder.Coder{elm}, Window: w}, nil

	case streamType:
		return nil, errors.Errorf("could not unmarshal stream type coder from %v, stream must be pair value", c)

	case "":
		// TODO(herohde) 11/27/2017: we still see CoderRefs from Dataflow. Handle that
		// case here, for now, so that the harness can use this logic.

		payload := c.GetSpec().GetPayload()

		var ref CoderRef
		if err := json.Unmarshal(payload, &ref); err != nil {
			return nil, errors.Wrapf(err, "could not unmarshal CoderRef from %v, failed to decode urn-less coder's payload \"%v\"", c, string(payload))
		}
		c, err := DecodeCoderRef(&ref)
		if err != nil {
			return nil, errors.Wrapf(err, "could not unmarshal CoderRef from %v, failed to decode CoderRef \"%v\"", c, string(payload))
		}
		return c, nil

	default:
		return nil, errors.Errorf("could not unmarshal coder from %v, unknown URN %v", c, urn)
	}
}

func (b *CoderUnmarshaller) peek(id string) (*pipepb.Coder, error) {
	c, ok := b.models[id]
	if !ok {
		return nil, errors.Errorf("coder with id %v not found", id)
	}
	return c, nil
}

func (b *CoderUnmarshaller) isCoGBKList(id string) ([]string, bool) {
	elm, err := b.peek(id)
	if err != nil {
		return nil, false
	}
	if elm.GetSpec().GetUrn() != urnLengthPrefixCoder {
		return nil, false
	}
	elm2, err := b.peek(elm.GetComponentCoderIds()[0])
	if err != nil {
		return nil, false
	}
	if elm2.GetSpec().GetUrn() != urnCoGBKList {
		return nil, false
	}
	return elm2.GetComponentCoderIds(), true
}

// CoderMarshaller incrementally builds a compact model representation of a set
// of coders. Identical coders are shared.
type CoderMarshaller struct {
	coders   map[string]*pipepb.Coder
	coder2id map[string]string // index of serialized coders to id to deduplicate
}

// NewCoderMarshaller returns a new CoderMarshaller.
func NewCoderMarshaller() *CoderMarshaller {
	return &CoderMarshaller{
		coders:   make(map[string]*pipepb.Coder),
		coder2id: make(map[string]string),
	}
}

// Add adds the given coder to the set and returns its id. Idempotent.
func (b *CoderMarshaller) Add(c *coder.Coder) string {
	switch c.Kind {
	case coder.Custom:
		ref, err := encodeCustomCoder(c.Custom)
		if err != nil {
			typeName := c.Custom.Name
			panic(errors.SetTopLevelMsgf(err, "Failed to encode custom coder for type %s. "+
				"Make sure the type was registered before calling beam.Init. For example: "+
				"beam.RegisterType(reflect.TypeOf((*TypeName)(nil)).Elem())", typeName))
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			panic(errors.Wrapf(err, "Failed to marshal custom coder %v", c))
		}
		inner := b.internCoder(&pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn:     urnCustomCoder,
				Payload: []byte(data),
			},
		})
		return b.internBuiltInCoder(urnLengthPrefixCoder, inner)

	case coder.KV:
		comp := b.AddMulti(c.Components)
		return b.internBuiltInCoder(urnKVCoder, comp...)

	case coder.CoGBK:
		comp := b.AddMulti(c.Components)

		value := comp[1]
		if len(comp) > 2 {
			// TODO(BEAM-490): don't inject union coder for CoGBK.

			union := b.internBuiltInCoder(urnCoGBKList, comp[1:]...)
			value = b.internBuiltInCoder(urnLengthPrefixCoder, union)
		}

		// SDKs always provide iterableCoder to runners, but can receive StateBackedIterables in return.
		stream := b.internBuiltInCoder(urnIterableCoder, value)
		return b.internBuiltInCoder(urnKVCoder, comp[0], stream)

	case coder.WindowedValue:
		comp := b.AddMulti(c.Components)
		comp = append(comp, b.AddWindowCoder(c.Window))
		return b.internBuiltInCoder(urnWindowedValueCoder, comp...)

	case coder.Bytes:
		// TODO(herohde) 6/27/2017: add length-prefix and not assume nested by context?
		return b.internBuiltInCoder(urnBytesCoder)

	case coder.Bool:
		return b.internBuiltInCoder(urnBoolCoder)

	case coder.VarInt:
		return b.internBuiltInCoder(urnVarIntCoder)

	case coder.Double:
		return b.internBuiltInCoder(urnDoubleCoder)

	case coder.String:
		return b.internBuiltInCoder(urnStringCoder)

	default:
		panic(fmt.Sprintf("Failed to marshal custom coder %v, unexpected coder kind: %v", c, c.Kind))
	}
}

// AddMulti adds the given coders to the set and returns their ids. Idempotent.
func (b *CoderMarshaller) AddMulti(list []*coder.Coder) []string {
	var ids []string
	for _, c := range list {
		ids = append(ids, b.Add(c))
	}
	return ids
}

// AddWindowCoder adds a window coder.
func (b *CoderMarshaller) AddWindowCoder(w *coder.WindowCoder) string {
	switch w.Kind {
	case coder.GlobalWindow:
		return b.internBuiltInCoder(urnGlobalWindow)
	case coder.IntervalWindow:
		return b.internBuiltInCoder(urnIntervalWindow)
	default:
		panic(fmt.Sprintf("Failed to add window coder %v, unexpected window kind: %v", w, w.Kind))
	}
}

// Build returns the set of model coders. Note that the map may be larger
// than the number of coders added, because component coders are included.
func (b *CoderMarshaller) Build() map[string]*pipepb.Coder {
	return b.coders
}

func (b *CoderMarshaller) internBuiltInCoder(urn string, components ...string) string {
	return b.internCoder(&pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn: urn,
		},
		ComponentCoderIds: components,
	})
}

func (b *CoderMarshaller) internCoder(coder *pipepb.Coder) string {
	key := proto.MarshalTextString(coder)
	if id, exists := b.coder2id[key]; exists {
		return id
	}

	id := fmt.Sprintf("c%v", len(b.coder2id))
	b.coder2id[key] = id
	b.coders[id] = coder
	return id
}
