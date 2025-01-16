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
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	v1pb "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
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
	urnParamWindowedValueCoder  = "beam:coder:param_windowed_value:v1"
	urnTimerCoder               = "beam:coder:timer:v1"
	urnRowCoder                 = "beam:coder:row:v1"
	urnNullableCoder            = "beam:coder:nullable:v1"

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
		urnRowCoder,
		urnNullableCoder,
		urnTimerCoder,
	}
}

// MarshalCoders marshals a list of coders into model coders.
func MarshalCoders(coders []*coder.Coder) ([]string, map[string]*pipepb.Coder, error) {
	b := NewCoderMarshaller()
	if ids, err := b.AddMulti(coders); err != nil {
		return nil, nil, err
	} else {
		return ids, b.Build(), nil
	}
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

	ret, err := b.makeCoder(id, c)
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
		return nil, errors.Errorf("could not unmarshal window coder: %w", err)
	}

	w, err := urnToWindowCoder(c.GetSpec().GetUrn())
	if err != nil {
		return nil, errors.SetTopLevelMsgf(err, "failed to unmarshal window coder %v", id)
	}
	b.windowCoders[id] = w
	return w, nil
}

func urnToWindowCoder(urn string) (*coder.WindowCoder, error) {
	switch urn {
	case urnGlobalWindow:
		return coder.NewGlobalWindow(), nil
	case urnIntervalWindow:
		return coder.NewIntervalWindow(), nil
	default:
		err := errors.Errorf("unexpected URN %v for window coder", urn)
		return nil, errors.WithContext(err, "translate URN to window coder")
	}
}

func (b *CoderUnmarshaller) makeCoder(id string, c *pipepb.Coder) (*coder.Coder, error) {
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
		elm, err := b.peek(id)
		if err != nil {
			return nil, errors.Errorf("could not unmarshal kv coder value component: %w", err)
		}

		switch elm.GetSpec().GetUrn() {
		case urnIterableCoder, urnStateBackedIterableCoder:
			iterElmID := elm.GetComponentCoderIds()[0]

			// TODO(https://github.com/apache/beam/issues/18032): If CoGBK with > 1 input, handle as special GBK. We expect
			// it to be encoded as CoGBK<K,LP<CoGBKList<V,W,..>>>. Remove this handling once
			// CoGBK has a first-class representation.

			// If the value is an iterable, and a special CoGBK type, then expand it to the real
			// CoGBK signature, instead of the special type.
			if ids, ok := b.isCoGBKList(iterElmID); ok {
				// CoGBK<K,V,W,..>

				values, err := b.Coders(ids)
				if err != nil {
					return nil, err
				}

				t := typex.New(typex.CoGBKType, append([]typex.FullType{key.T}, coder.Types(values)...)...)
				return &coder.Coder{Kind: coder.CoGBK, T: t, Components: append([]*coder.Coder{key}, values...)}, nil
			}
			// It's valid to have a KV<k,Iter<v>> without being a CoGBK, and validating if we need to change to
			// a CoGBK is done at the DataSource, since that's when we can check against the downstream nodes.
		}

		value, err := b.Coder(id)
		if err != nil {
			return nil, err
		}

		t := typex.New(typex.KVType, key.T, value.T)
		return &coder.Coder{Kind: coder.KV, T: t, Components: []*coder.Coder{key, value}}, nil

	case urnLengthPrefixCoder:
		if len(components) != 1 {
			return nil, errors.Errorf("could not unmarshal length prefix coder from %v, want a single sub component but have %d", c, len(components))
		}

		sub, err := b.peek(components[0])
		if err != nil {
			return nil, errors.Errorf("could not unmarshal length prefix coder component: %w", err)
		}

		// No payload means this coder was length prefixed by the runner
		// but is likely self describing - AKA a beam coder.
		if len(sub.GetSpec().GetPayload()) == 0 {
			return b.makeCoder(components[0], sub)
		}
		// TODO(lostluck) 2018/10/17: Make this strict again, once dataflow can use
		// the portable pipeline model directly (BEAM-2885)
		switch u := sub.GetSpec().GetUrn(); u {
		case "", urnCustomCoder:
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
			cc := &coder.Coder{Kind: coder.Custom, T: t, Custom: custom}
			return cc, nil
		case urnBytesCoder, urnStringCoder: // implicitly length prefixed types.
			return b.makeCoder(components[0], sub)
		default:
			// Handle Length prefixing dictated by the runner.
			cc, err := b.makeCoder(components[0], sub)
			if err != nil {
				return nil, err
			}
			return &coder.Coder{Kind: coder.LP, T: cc.T, Components: []*coder.Coder{cc}}, nil
		}

	case urnWindowedValueCoder, urnParamWindowedValueCoder:
		if len(components) != 2 {
			return nil, errors.Errorf("could not unmarshal windowed value coder from %v, expected two components but got %d", c, len(components))
		}

		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		w, err := b.WindowCoder(components[1])
		if err != nil {
			return nil, errors.Errorf("could not unmarshal window coder: %w", err)
		}
		t := typex.New(typex.WindowedValueType, elm.T)
		wvc := &coder.Coder{Kind: coder.WindowedValue, T: t, Components: []*coder.Coder{elm}, Window: w}
		if urn == urnWindowedValueCoder {
			return wvc, nil
		}
		wvc.Kind = coder.ParamWindowedValue
		wvc.Window.Payload = string(c.GetSpec().GetPayload())
		return wvc, nil

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

	case urnIterableCoder, urnStateBackedIterableCoder:
		if len(components) != 1 {
			return nil, errors.Errorf("could not unmarshal iterable coder from %v, expected one component but got %d", c, len(components))
		}
		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		return coder.NewI(elm), nil
	case urnTimerCoder:
		if len(components) != 2 {
			return nil, errors.Errorf("could not unmarshal timer coder from %v, expected two component but got %d", c, len(components))
		}
		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		w, err := b.WindowCoder(components[1])
		if err != nil {
			return nil, errors.Errorf("could not unmarshal window coder for timer: %w", err)
		}
		return coder.NewT(elm, w), nil
	case urnRowCoder:
		var s pipepb.Schema
		if err := proto.Unmarshal(c.GetSpec().GetPayload(), &s); err != nil {
			return nil, err
		}
		t, err := schema.ToType(&s)
		if err != nil {
			return nil, err
		}
		return coder.NewR(typex.New(t)), nil
	case urnNullableCoder:
		if len(components) != 1 {
			return nil, errors.Errorf("could not unmarshal nullable coder from %v, expected one component but got %d", c, len(components))
		}
		elm, err := b.Coder(components[0])
		if err != nil {
			return nil, err
		}
		return coder.NewN(elm), nil
	case urnIntervalWindow:
		return coder.NewIntervalWindowCoder(), nil

	// Special handling for the global window coder so it can be treated as
	// a general coder. Generally window coders are not used outside of
	// specific contexts, but this enables improved testing.
	// Window types are not permitted to be fulltypes, so
	// we use assignably equivalent anonymous struct types.
	case urnGlobalWindow:
		w, err := b.WindowCoder(id)
		if err != nil {
			return nil, errors.Errorf("could not unmarshal global window coder: %w", err)
		}
		return &coder.Coder{Kind: coder.Window, T: typex.New(reflect.TypeOf((*struct{})(nil)).Elem()), Window: w}, nil
	default:
		return nil, errors.Errorf("could not unmarshal coder from %v, unknown URN %v", c, urn)
	}
}

func (b *CoderUnmarshaller) peek(id string) (*pipepb.Coder, error) {
	c, ok := b.models[id]
	if !ok {
		return nil, errors.Errorf("(peek) coder with id %v not found", id)
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

	Namespace string // Namespace for xlang coders.
}

// NewCoderMarshaller returns a new CoderMarshaller.
func NewCoderMarshaller() *CoderMarshaller {
	return &CoderMarshaller{
		coders:   make(map[string]*pipepb.Coder),
		coder2id: make(map[string]string),
	}
}

// Add adds the given coder to the set and returns its id. Idempotent.
func (b *CoderMarshaller) Add(c *coder.Coder) (string, error) {
	switch c.Kind {
	case coder.Custom:
		ref, err := encodeCustomCoder(c.Custom)
		if err != nil {
			return "", errors.SetTopLevelMsgf(err, "failed to encode custom coder %s for TypeName %s. "+
				"Make sure the type was registered before calling beam.Init. For example: "+
				"beam.RegisterType(reflect.TypeOf((*TypeName)(nil)).Elem()). Some types, like maps, slices, arrays, channels, and functions cannot be registered as types.", c, c.Custom.Type)
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal custom coder %v", c)
		}
		inner := b.internCoder(&pipepb.Coder{
			Spec: &pipepb.FunctionSpec{
				Urn:     urnCustomCoder,
				Payload: []byte(data),
			},
		})
		return b.internBuiltInCoder(urnLengthPrefixCoder, inner), nil

	case coder.KV:
		comp, err := b.AddMulti(c.Components)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal KV coder %v", c)
		}
		return b.internBuiltInCoder(urnKVCoder, comp...), nil

	case coder.Nullable:
		comp, err := b.AddMulti(c.Components)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal Nullable coder %v", c)
		}
		return b.internBuiltInCoder(urnNullableCoder, comp...), nil

	case coder.CoGBK:
		comp, err := b.AddMulti(c.Components)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal CoGBK coder %v", c)
		}
		value := comp[1]
		if len(comp) > 2 {
			// TODO(https://github.com/apache/beam/issues/18032): don't inject union coder for CoGBK.

			union := b.internBuiltInCoder(urnCoGBKList, comp[1:]...)
			value = b.internBuiltInCoder(urnLengthPrefixCoder, union)
		}

		// SDKs always provide iterableCoder to runners, but can receive StateBackedIterables in return.
		stream := b.internBuiltInCoder(urnIterableCoder, value)
		return b.internBuiltInCoder(urnKVCoder, comp[0], stream), nil

	case coder.WindowedValue:
		comp := []string{}
		if ids, err := b.AddMulti(c.Components); err != nil {
			return "", errors.Wrapf(err, "failed to marshal window coder %v", c)
		} else {
			comp = append(comp, ids...)
		}
		if id, err := b.AddWindowCoder(c.Window); err != nil {
			return "", errors.Wrapf(err, "failed to marshal window coder %v", c)
		} else {
			comp = append(comp, id)
		}
		return b.internBuiltInCoder(urnWindowedValueCoder, comp...), nil

	case coder.Bytes:
		// TODO(herohde) 6/27/2017: add length-prefix and not assume nested by context?
		return b.internBuiltInCoder(urnBytesCoder), nil

	case coder.Bool:
		return b.internBuiltInCoder(urnBoolCoder), nil

	case coder.VarInt:
		return b.internBuiltInCoder(urnVarIntCoder), nil

	case coder.Double:
		return b.internBuiltInCoder(urnDoubleCoder), nil

	case coder.String:
		return b.internBuiltInCoder(urnStringCoder), nil

	case coder.IW:
		return b.internBuiltInCoder(urnIntervalWindow), nil

	case coder.Row:
		rt := c.T.Type()
		s, err := schema.FromType(rt)
		if err != nil {
			return "", errors.SetTopLevelMsgf(err, "failed to convert type %v to a schema.", rt)
		}
		return b.internRowCoder(s), nil

	case coder.Timer:
		comp := []string{}
		ids, err := b.AddMulti(c.Components)
		if err != nil {
			return "", errors.SetTopLevelMsgf(err, "failed to marshal timer coder %v", c)
		}
		comp = append(comp, ids...)

		id, err := b.AddWindowCoder(c.Window)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal window coder %v", c)
		}
		comp = append(comp, id)

		return b.internBuiltInCoder(urnTimerCoder, comp...), nil

	case coder.Iterable:
		comp, err := b.AddMulti(c.Components)
		if err != nil {
			return "", errors.Wrapf(err, "failed to marshal iterable coder %v", c)
		}
		return b.internBuiltInCoder(urnIterableCoder, comp...), nil

	default:
		err := errors.Errorf("unexpected coder kind: %v", c.Kind)
		return "", errors.WithContextf(err, "failed to marshal coder %v", c)
	}
}

// AddMulti adds the given coders to the set and returns their ids. Idempotent.
func (b *CoderMarshaller) AddMulti(list []*coder.Coder) ([]string, error) {
	var ids []string
	for _, c := range list {
		if id, err := b.Add(c); err != nil {
			return nil, errors.Wrapf(err, "failed to marshal the coder %v.", c)
		} else {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// AddWindowCoder adds a window coder.
func (b *CoderMarshaller) AddWindowCoder(w *coder.WindowCoder) (string, error) {
	switch w.Kind {
	case coder.GlobalWindow:
		return b.internBuiltInCoder(urnGlobalWindow), nil
	case coder.IntervalWindow:
		return b.internBuiltInCoder(urnIntervalWindow), nil
	default:
		err := errors.Errorf("window coder with unexpected type %v", w.Kind)
		return "", errors.WithContextf(err, "failed to unmarshal window coder %v", w)
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

func (b *CoderMarshaller) internRowCoder(schema *pipepb.Schema) string {
	payload := protox.MustEncode(schema)
	return b.internCoder(&pipepb.Coder{
		Spec: &pipepb.FunctionSpec{
			Urn:     urnRowCoder,
			Payload: payload,
		},
	})
}

func (b *CoderMarshaller) internCoder(coder *pipepb.Coder) string {
	key := coder.String()
	if id, exists := b.coder2id[(key)]; exists {
		return id
	}

	var id string
	if b.Namespace == "" {
		id = fmt.Sprintf("c%v", len(b.coder2id))
	} else {
		id = fmt.Sprintf("c%v@%v", len(b.coder2id), b.Namespace)
	}
	b.coder2id[string(key)] = id
	b.coders[id] = coder
	return id
}
