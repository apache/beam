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
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/schema"
	v1pb "github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// TODO(herohde) 7/17/2018: move CoderRef to dataflowlib once Dataflow
// sends back pure coder protos in the process bundle descriptors.

// CoderRef defines the (structured) Coder in serializable form. It is
// an artifact of the CloudObject encoding.
type CoderRef struct {
	Type                 string      `json:"@type,omitempty"`
	Components           []*CoderRef `json:"component_encodings,omitempty"`
	IsWrapper            bool        `json:"is_wrapper,omitempty"`
	IsPairLike           bool        `json:"is_pair_like,omitempty"`
	IsStreamLike         bool        `json:"is_stream_like,omitempty"`
	PipelineProtoCoderID string      `json:"pipeline_proto_coder_id,omitempty"`
}

// Exported types are used for translation lookup.
const (
	windowedValueType = "kind:windowed_value"
	bytesType         = "kind:bytes"
	boolType          = "kind:bool"
	varIntType        = "kind:varint"
	doubleType        = "kind:double"
	streamType        = "kind:stream"
	pairType          = "kind:pair"
	lengthPrefixType  = "kind:length_prefix"
	rowType           = "kind:row"

	globalWindowType   = "kind:global_window"
	intervalWindowType = "kind:interval_window"

	cogbklistType = "kind:cogbklist" // CoGBK representation. Not a coder.
	stringType    = "kind:string"    // Not a classical Dataflow kind.
)

// WrapIterable adds an iterable (stream) coder for Dataflow side input.
func WrapIterable(c *CoderRef) *CoderRef {
	return &CoderRef{Type: streamType, Components: []*CoderRef{c}, IsStreamLike: true}
}

// WrapWindowed adds a windowed coder for Dataflow collections.
func WrapWindowed(c *CoderRef, wc *coder.WindowCoder) *CoderRef {
	w, err := encodeWindowCoder(wc)
	if err != nil {
		panic(err)
	}
	return &CoderRef{Type: windowedValueType, Components: []*CoderRef{c, w}, IsWrapper: true}
}

// EncodeCoderRefs returns the encoded forms understood by the runner.
func EncodeCoderRefs(list []*coder.Coder) ([]*CoderRef, error) {
	var refs []*CoderRef
	for _, c := range list {
		ref, err := EncodeCoderRef(c)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// EncodeCoderRef returns the encoded form understood by the runner.
func EncodeCoderRef(c *coder.Coder) (*CoderRef, error) {
	switch c.Kind {
	case coder.Custom:
		ref, err := encodeCustomCoder(c.Custom)
		if err != nil {
			return nil, err
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			return nil, err
		}
		return &CoderRef{
			Type:       lengthPrefixType,
			Components: []*CoderRef{{Type: data, PipelineProtoCoderID: c.Custom.ID}},
		}, nil

	case coder.KV:
		if len(c.Components) != 2 {
			return nil, errors.Errorf("bad KV: %v", c)
		}

		key, err := EncodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		value, err := EncodeCoderRef(c.Components[1])
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: pairType, Components: []*CoderRef{key, value}, IsPairLike: true}, nil

	case coder.CoGBK:
		if len(c.Components) < 2 {
			return nil, errors.Errorf("bad CoGBK: %v", c)
		}

		refs, err := EncodeCoderRefs(c.Components)
		if err != nil {
			return nil, err
		}

		value := refs[1]
		if len(c.Components) > 2 {
			// TODO(BEAM-490): don't inject union coder for CoGBK.

			union := &CoderRef{Type: cogbklistType, Components: refs[1:]}
			value = &CoderRef{Type: lengthPrefixType, Components: []*CoderRef{union}}
		}

		stream := &CoderRef{Type: streamType, Components: []*CoderRef{value}, IsStreamLike: true}
		return &CoderRef{Type: pairType, Components: []*CoderRef{refs[0], stream}, IsPairLike: true}, nil

	case coder.WindowedValue:
		if len(c.Components) != 1 || c.Window == nil {
			return nil, errors.Errorf("bad windowed value: %v", c)
		}

		elm, err := EncodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		w, err := encodeWindowCoder(c.Window)
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: windowedValueType, Components: []*CoderRef{elm, w}, IsWrapper: true}, nil

	case coder.Bytes:
		return &CoderRef{Type: bytesType}, nil

	case coder.Bool:
		return &CoderRef{Type: boolType}, nil

	case coder.VarInt:
		return &CoderRef{Type: varIntType}, nil

	case coder.Double:
		return &CoderRef{Type: doubleType}, nil

	case coder.String:
		return &CoderRef{
			Type:       lengthPrefixType,
			Components: []*CoderRef{{Type: stringType, PipelineProtoCoderID: c.ID}},
		}, nil

	case coder.Row:
		schm, err := schema.FromType(c.T.Type())
		if err != nil {
			return nil, err
		}
		data, err := protox.EncodeBase64(schm)
		if err != nil {
			return nil, err
		}
		return &CoderRef{
			Type:       rowType,
			Components: []*CoderRef{{Type: data}},
		}, nil

	default:
		return nil, errors.Errorf("bad coder kind: %v", c.Kind)
	}
}

// DecodeCoderRefs extracts usable coders from the encoded runner form.
func DecodeCoderRefs(list []*CoderRef) ([]*coder.Coder, error) {
	var ret []*coder.Coder
	for _, ref := range list {
		c, err := DecodeCoderRef(ref)
		if err != nil {
			return nil, err
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// DecodeCoderRef extracts a usable coder from the encoded runner form.
func DecodeCoderRef(c *CoderRef) (*coder.Coder, error) {
	switch c.Type {
	case bytesType:
		return coder.NewBytes(), nil

	case boolType:
		return coder.NewBool(), nil

	case varIntType:
		return coder.NewVarInt(), nil

	case doubleType:
		return coder.NewDouble(), nil

	case stringType:
		return coder.NewString(), nil

	case pairType:
		if len(c.Components) != 2 {
			return nil, errors.Errorf("bad pair: %+v", c)
		}

		key, err := DecodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}

		elm := c.Components[1]
		kind := coder.KV
		root := typex.KVType

		isGBK := elm.Type == streamType
		if isGBK {
			elm = elm.Components[0]
			kind = coder.CoGBK
			root = typex.CoGBKType

			// TODO(BEAM-490): If CoGBK with > 1 input, handle as special GBK. We expect
			// it to be encoded as CoGBK<K,LP<Union<V,W,..>>. Remove this handling once
			// CoGBK has a first-class representation.

			if refs, ok := isCoGBKList(elm); ok {
				values, err := DecodeCoderRefs(refs)
				if err != nil {
					return nil, err
				}

				t := typex.New(root, append([]typex.FullType{key.T}, coder.Types(values)...)...)
				return &coder.Coder{Kind: kind, T: t, Components: append([]*coder.Coder{key}, values...)}, nil
			}
		}

		value, err := DecodeCoderRef(elm)
		if err != nil {
			return nil, err
		}

		t := typex.New(root, key.T, value.T)
		return &coder.Coder{Kind: kind, T: t, Components: []*coder.Coder{key, value}}, nil

	case lengthPrefixType:
		if len(c.Components) != 1 {
			return nil, errors.Errorf("bad length prefix: %+v", c)
		}

		subC := c.Components[0]
		switch subC.Type {
		case stringType: // Needs special handling if wrapped by dataflow.
			return coder.NewString(), nil
		default:
			return decodeDataflowCustomCoder(subC.Type)
		}

	case windowedValueType:
		if len(c.Components) != 2 {
			return nil, errors.Errorf("bad windowed value: %+v", c)
		}

		elm, err := DecodeCoderRef(c.Components[0])
		if err != nil {
			return nil, err
		}
		w, err := decodeWindowCoder(c.Components[1])
		if err != nil {
			return nil, err
		}
		t := typex.New(typex.WindowedValueType, elm.T)

		return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: []*coder.Coder{elm}, Window: w}, nil

	case streamType:
		return nil, errors.Errorf("stream must be pair value: %+v", c)

	case rowType:
		subC := c.Components[0]
		schm := &pipepb.Schema{}
		if err := protox.DecodeBase64(subC.Type, schm); err != nil {
			return nil, err
		}
		t, err := schema.ToType(schm)
		if err != nil {
			return nil, err
		}
		return &coder.Coder{Kind: coder.Row, T: typex.New(t)}, nil

	default:
		return nil, errors.Errorf("custom coders must be length prefixed: %+v", c)
	}
}

func decodeDataflowCustomCoder(payload string) (*coder.Coder, error) {
	var ref v1pb.CustomCoder
	if err := protox.DecodeBase64(payload, &ref); err != nil {
		return nil, errors.Wrapf(err, "base64 decode for %v failed", payload)
	}
	custom, err := decodeCustomCoder(&ref)
	if err != nil {
		return nil, err
	}
	t := typex.New(custom.Type)
	return &coder.Coder{Kind: coder.Custom, T: t, Custom: custom}, nil
}

func isCoGBKList(ref *CoderRef) ([]*CoderRef, bool) {
	if ref.Type != lengthPrefixType {
		return nil, false
	}
	ref2 := ref.Components[0]
	if ref2.Type != cogbklistType {
		return nil, false
	}
	return ref2.Components, true
}

// encodeWindowCoder translates the preprocessed representation of a Beam coder
// into the wire representation, capturing the underlying types used by
// the coder.
func encodeWindowCoder(w *coder.WindowCoder) (*CoderRef, error) {
	switch w.Kind {
	case coder.GlobalWindow:
		return &CoderRef{Type: globalWindowType}, nil
	case coder.IntervalWindow:
		return &CoderRef{Type: intervalWindowType}, nil
	default:
		return nil, errors.Errorf("bad window kind: %v", w.Kind)
	}
}

// decodeWindowCoder receives the wire representation of a Beam coder, extracting
// the preprocessed representation, expanding all types used by the coder.
func decodeWindowCoder(w *CoderRef) (*coder.WindowCoder, error) {
	switch w.Type {
	case globalWindowType:
		return coder.NewGlobalWindow(), nil
	case intervalWindowType:
		return coder.NewIntervalWindow(), nil
	default:
		return nil, errors.Errorf("bad window: %v", w.Type)
	}
}
