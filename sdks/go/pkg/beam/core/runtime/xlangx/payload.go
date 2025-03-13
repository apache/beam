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

package xlangx

import (
	"bytes"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
)

// CreateExternalConfigurationPayload takes a native Go struct and returns an
// ExternalConfigurationPayload proto with the struct encoded as a Row and its
// associated schema.
func CreateExternalConfigurationPayload(pl any) (*pipepb.ExternalConfigurationPayload, error) {
	rt := reflect.TypeOf(pl)

	// Encode payload value as a Row.
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		err = errors.WithContext(err, "creating Row encoder for payload")
		return nil, errors.WithContextf(err, "encoding external payload %v", pl)
	}
	var buf bytes.Buffer
	if err := enc(pl, &buf); err != nil {
		err = errors.WithContext(err, "encoding payload as Row")
		return nil, errors.WithContextf(err, "encoding external payload %v", pl)
	}

	// Convert payload type into Schema representation.
	scm, err := schema.FromType(rt)
	if err != nil {
		err = errors.WithContext(err, "creating schema for payload")
		return nil, errors.WithContextf(err, "encoding external payload %v", pl)
	}

	// Put schema and row into payload proto, and marshal it.
	ecp := &pipepb.ExternalConfigurationPayload{
		Schema:  scm,
		Payload: buf.Bytes(),
	}
	return ecp, nil
}

// EncodeStructPayload takes a native Go struct and returns a marshaled
// ExternalConfigurationPayload proto, containing a Schema representation of
// the original type and the original value encoded as a Row. This is intended
// to be used as the expansion payload for an External transform.
func EncodeStructPayload(pl any) ([]byte, error) {
	ecp, err := CreateExternalConfigurationPayload(pl)
	if err != nil {
		return []byte{}, err
	}

	plBytes, err := proto.Marshal(ecp)
	if err != nil {
		err = errors.Wrapf(err, "failed to marshal payload as proto")
		return []byte{}, errors.WithContextf(err, "encoding external payload %v", pl)
	}

	return plBytes, nil
}

// DecodeStructPayload takes a marshaled ExternalConfigurationPayload proto
// and returns a native Go struct, with its type converted from the Schema
// representation and its value decoded from the Row.
func DecodeStructPayload(plBytes []byte) (any, error) {
	// Unmarshal payload proto.
	ecp := &pipepb.ExternalConfigurationPayload{}
	if err := proto.Unmarshal(plBytes, ecp); err != nil {
		err = errors.WithContext(err, "failed to unmarshal the payload proto")
		return nil, errors.WithContext(err, "decoding external payload")
	}

	// Convert Schema representation into payload type.
	rt, err := schema.ToType(ecp.GetSchema())
	if err != nil {
		err = errors.WithContextf(err, "converting schema to payload type. schema: %v", ecp.GetSchema())
		return nil, errors.WithContext(err, "decoding external payload")
	}

	// Decode a Row to payload value.
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		err = errors.WithContextf(err, "creating Row decoder for type %v", rt)
		return nil, errors.WithContext(err, "decoding external payload")
	}
	buf := bytes.NewBuffer(ecp.Payload)
	val, err := dec(buf)
	if err != nil {
		err = errors.WithContext(err, "decoding Row to payload")
		return nil, errors.WithContextf(err, "decoding external payload")
	}

	return val, nil
}
