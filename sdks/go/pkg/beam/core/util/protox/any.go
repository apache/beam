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

package protox

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"google.golang.org/protobuf/proto"
	protobuf "google.golang.org/protobuf/types/known/anypb"
	protobufw "google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	bytesValueTypeURL = "type.googleapis.com/google.protobuf.BytesValue"
)

// Unpack decodes a proto.
func Unpack(data *protobuf.Any, url string, ret proto.Message) error {
	if data.TypeUrl != url {
		return errors.Errorf("bad type: %v, want %v", data.TypeUrl, url)
	}
	return proto.Unmarshal(data.Value, ret)
}

// UnpackProto decodes a BytesValue wrapped proto.
func UnpackProto(data *protobuf.Any, ret proto.Message) error {
	buf, err := UnpackBytes(data)
	if err != nil {
		return err
	}
	return proto.Unmarshal(buf, ret)
}

// PackProto encodes a proto message and wraps into a BytesValue.
func PackProto(in proto.Message) (*protobuf.Any, error) {
	b, err := proto.Marshal(in)
	if err != nil {
		return nil, err
	}

	return PackBytes(b)
}

// UnpackBase64Proto decodes a BytesValue + base64 wrapped proto.
func UnpackBase64Proto(data *protobuf.Any, ret proto.Message) error {
	buf, err := UnpackBytes(data)
	if err != nil {
		return err
	}
	return DecodeBase64(string(buf), ret)
}

// PackBase64Proto encodes a proto message into a base64-encoded BytesValue message.
func PackBase64Proto(in proto.Message) (*protobuf.Any, error) {
	data, err := EncodeBase64(in)
	if err != nil {
		return nil, err
	}
	return PackBytes([]byte(data))
}

// UnpackBytes removes the BytesValue wrapper.
func UnpackBytes(data *protobuf.Any) ([]byte, error) {
	if data.TypeUrl != bytesValueTypeURL {
		return nil, errors.Errorf("bad type: %v, want %v", data.TypeUrl, bytesValueTypeURL)
	}

	var buf protobufw.BytesValue
	if err := proto.Unmarshal(data.Value, &buf); err != nil {
		return nil, errors.Wrap(err, "BytesValue unmarshal failed")
	}
	return buf.Value, nil
}

// PackBytes applies a BytesValue wrapper to the supplied bytes.
func PackBytes(in []byte) (*protobuf.Any, error) {
	var buf protobufw.BytesValue
	buf.Value = in
	b, err := proto.Marshal(&buf)
	if err != nil {
		return nil, err
	}

	var out protobuf.Any
	out.TypeUrl = bytesValueTypeURL
	out.Value = b
	return &out, nil
}
