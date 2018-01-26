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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/golang/protobuf/proto"
)

const (
	// Model constants

	urnBytesCoder         = "urn:beam:coders:bytes:0.1"
	urnVarIntCoder        = "urn:beam:coders:varint:0.1"
	urnLengthPrefixCoder  = "urn:beam:coders:length_prefix:0.1"
	urnKVCoder            = "urn:beam:coders:kv:0.1"
	urnStreamCoder        = "urn:beam:coders:stream:0.1"
	urnWindowedValueCoder = "urn:beam:coders:windowed_value:0.1"

	urnGlobalWindow         = "urn:beam:coders:global_window:0.1"
	urnIntervalWindowsCoder = "urn:beam:coders:interval_window:0.1"

	// SDK constants

	urnCustomCoder = "urn:beam:go:coders:custom:v1"
)

// MarshalCoders marshals a list of coders into model coders.
func MarshalCoders(coders []*coder.Coder) ([]string, map[string]*pb.Coder) {
	b := NewCoderMarshaller()
	ids := b.AddMulti(coders)
	return ids, b.Build()
}

// UnmarshalCoders unmarshals coders.
func UnmarshalCoders(ids []string, m map[string]*pb.Coder) ([]*coder.Coder, error) {
	b := NewCoderUnmarshaller(m)

	var coders []*coder.Coder
	for _, id := range ids {
		c, err := b.Coder(id)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal coder %v: %v", id, err)
		}
		coders = append(coders, c)
	}
	return coders, nil
}

// CoderUnmarshaller is an incremental unmarshaller of model coders. Identical
// coders are shared.
type CoderUnmarshaller struct {
	models map[string]*pb.Coder

	coders  map[string]*coder.Coder
	windows map[string]*window.Window
}

// NewCoderUnmarshaller returns a new CoderUnmarshaller.
func NewCoderUnmarshaller(m map[string]*pb.Coder) *CoderUnmarshaller {
	return &CoderUnmarshaller{
		models:  m,
		coders:  make(map[string]*coder.Coder),
		windows: make(map[string]*window.Window),
	}
}

// Coder unmarshals a coder with the given id.
func (b *CoderUnmarshaller) Coder(id string) (*coder.Coder, error) {
	if c, exists := b.coders[id]; exists {
		return c, nil
	}
	c, ok := b.models[id]
	if !ok {
		return nil, fmt.Errorf("coder with id %v not found", id)
	}

	ret, err := b.makeCoder(c)
	if err != nil {
		return nil, err
	}

	b.coders[id] = ret
	return ret, nil
}

// Coder unmarshals a window with the given id.
func (b *CoderUnmarshaller) Window(id string) (*window.Window, error) {
	if w, exists := b.windows[id]; exists {
		return w, nil
	}

	c, err := b.peek(id)
	if err != nil {
		return nil, err
	}

	urn := c.GetSpec().GetSpec().GetUrn()
	switch urn {
	case urnGlobalWindow:
		w := window.NewGlobalWindow()
		b.windows[id] = w
		return w, nil

	default:
		panic(fmt.Sprintf("Unexpected window coder: %v", urn))
	}
}

func (b *CoderUnmarshaller) makeCoder(c *pb.Coder) (*coder.Coder, error) {
	urn := c.GetSpec().GetSpec().GetUrn()
	components := c.GetComponentCoderIds()

	switch urn {
	case urnBytesCoder:
		return coder.NewBytes(), nil

	case urnVarIntCoder:
		return coder.NewVarInt(), nil

	case urnKVCoder:
		if len(components) != 2 {
			return nil, fmt.Errorf("bad pair: %v", c)
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
		isGBK := elm.GetSpec().GetSpec().GetUrn() == urnStreamCoder
		if isGBK {
			id = elm.GetComponentCoderIds()[0]
			kind = coder.CoGBK
			root = typex.CoGBKType
		}

		value, err := b.Coder(id)
		if err != nil {
			return nil, err
		}

		// TODO: if value is union coder

		t := typex.New(root, key.T, value.T)

		return &coder.Coder{Kind: kind, T: t, Components: []*coder.Coder{key, value}}, nil

	case urnLengthPrefixCoder:
		if len(components) != 1 {
			return nil, fmt.Errorf("bad length prefix: %v", c)
		}

		elm, err := b.peek(components[0])
		if err != nil {
			return nil, err
		}
		if elm.GetSpec().GetSpec().GetUrn() != urnCustomCoder {
			// TODO(herohde) 11/17/2017: revisit this restriction
			return nil, fmt.Errorf("expected length prefix of custom coder only: %v", elm)
		}

		var ref v1.CustomCoder
		if err := protox.DecodeBase64(string(elm.GetSpec().GetSpec().GetPayload()), &ref); err != nil {
			return nil, err
		}
		custom, err := decodeCustomCoder(&ref)
		if err != nil {
			return nil, err
		}
		t := typex.New(custom.Type)
		return &coder.Coder{Kind: coder.Custom, T: t, Custom: custom}, nil

	case urnWindowedValueCoder:
		if len(components) != 2 {
			return nil, fmt.Errorf("bad windowed value: %v", c)
		}

		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		w, err := b.Window(components[1])
		if err != nil {
			return nil, err
		}
		t := typex.New(typex.WindowedValueType, elm.T)
		return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: []*coder.Coder{elm}, Window: w}, nil

	case streamType:
		return nil, fmt.Errorf("stream must be pair value: %v", c)

	case "":
		// TODO(herohde) 11/27/2017: we still see CoderRefs from Dataflow. Handle that
		// case here, for now, so that the harness can use this logic.

		payload := c.GetSpec().GetSpec().GetPayload()

		var ref CoderRef
		if err := json.Unmarshal(payload, &ref); err != nil {
			return nil, fmt.Errorf("failed to decode urn-less coder payload \"%v\": %v", string(payload), err)
		}
		c, err := DecodeCoderRef(&ref)
		if err != nil {
			return nil, fmt.Errorf("failed to translate coder \"%v\": %v", string(payload), err)
		}
		return c, nil

	default:
		return nil, fmt.Errorf("custom coders must be length prefixed: %v", c)
	}
}

func (b *CoderUnmarshaller) peek(id string) (*pb.Coder, error) {
	c, ok := b.models[id]
	if !ok {
		return nil, fmt.Errorf("coder with id %v not found", id)
	}
	return c, nil
}

// CoderMarshaller incrementally builds a compact model representation of a set
// of coders. Identical coders are shared.
type CoderMarshaller struct {
	coders   map[string]*pb.Coder
	coder2id map[string]string // index of serialized coders to id to deduplicate
}

// NewCoderMarshaller returns a new CoderMarshaller.
func NewCoderMarshaller() *CoderMarshaller {
	return &CoderMarshaller{
		coders:   make(map[string]*pb.Coder),
		coder2id: make(map[string]string),
	}
}

// Add adds the given coder to the set and returns its id. Idempotent.
func (b *CoderMarshaller) Add(c *coder.Coder) string {
	switch c.Kind {
	case coder.Custom:
		ref, err := encodeCustomCoder(c.Custom)
		if err != nil {
			panic(fmt.Sprintf("failed to encode custom coder: %v", err))
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal custom coder: %v", err))
		}
		inner := b.internCoder(&pb.Coder{
			Spec: &pb.SdkFunctionSpec{
				Spec: &pb.FunctionSpec{
					Urn:     urnCustomCoder,
					Payload: []byte(data),
				},
				// TODO(BEAM-3204): coders should not have environments.
			},
		})
		return b.internBuiltInCoder(urnLengthPrefixCoder, inner)

	case coder.KV:
		comp := b.AddMulti(c.Components)
		return b.internBuiltInCoder(urnKVCoder, comp...)

	case coder.CoGBK:
		comp := b.AddMulti(c.Components)
		stream := b.internBuiltInCoder(urnStreamCoder, comp[1])
		return b.internBuiltInCoder(urnKVCoder, comp[0], stream)

	case coder.WindowedValue:
		comp := b.AddMulti(c.Components)
		comp = append(comp, b.AddWindow(c.Window))
		return b.internBuiltInCoder(urnWindowedValueCoder, comp...)

	case coder.Bytes:
		// TODO(herohde) 6/27/2017: add length-prefix and not assume nested by context?
		return b.internBuiltInCoder(urnBytesCoder)

	case coder.VarInt:
		return b.internBuiltInCoder(urnVarIntCoder)

	default:
		panic(fmt.Sprintf("Unexpected coder kind: %v", c.Kind))
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

// AddWindow adds a window coder.
func (b *CoderMarshaller) AddWindow(w *window.Window) string {
	switch w.Kind() {
	case window.GlobalWindow:
		return b.internBuiltInCoder(urnGlobalWindow)

	default:
		panic(fmt.Sprintf("Unexpected window kind: %v", w.Kind()))
	}
}

// Build returns the set of model coders. Note that the map may be larger
// than the number of coders added, because component coders are included.
func (b *CoderMarshaller) Build() map[string]*pb.Coder {
	return b.coders
}

func (b *CoderMarshaller) internBuiltInCoder(urn string, components ...string) string {
	return b.internCoder(&pb.Coder{
		Spec: &pb.SdkFunctionSpec{
			Spec: &pb.FunctionSpec{
				Urn: urn,
			},
		},
		ComponentCoderIds: components,
	})
}

func (b *CoderMarshaller) internCoder(coder *pb.Coder) string {
	key := proto.MarshalTextString(coder)
	if id, exists := b.coder2id[key]; exists {
		return id
	}

	id := fmt.Sprintf("c%v", len(b.coder2id))
	b.coder2id[key] = id
	b.coders[id] = coder
	return id
}
