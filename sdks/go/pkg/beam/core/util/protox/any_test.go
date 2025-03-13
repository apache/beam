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
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
	protobufw "google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtoPackingInvertibility(t *testing.T) {
	var buf protobufw.BytesValue
	buf.Value = []byte("Here is some data")

	msg, err := PackProto(&buf)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	var res protobufw.BytesValue
	err = UnpackProto(msg, &res)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !proto.Equal(&res, &buf) {
		t.Errorf("Got %v, wanted %v", &res, &buf)
	}

}

func TestProto64PackingInvertibility(t *testing.T) {
	var buf protobufw.BytesValue
	buf.Value = []byte("Here is some data")

	any, err := PackBase64Proto(&buf)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	var res protobufw.BytesValue
	err = UnpackBase64Proto(any, &res)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !proto.Equal(&res, &buf) {
		t.Errorf("Got %v, wanted %v", &res, &buf)
	}
}

func TestBytesPackingInvertibility(t *testing.T) {
	data := []byte("Here is some data")

	any, err := PackBytes(data)
	if err != nil {
		t.Errorf("Failed to pack data: %v", err)
	}

	b, err := UnpackBytes(any)
	if err != nil {
		t.Errorf("Failed to unpack data: %v", err)
	}

	if !bytes.Equal(b, data) {
		t.Errorf("Got %v, wanted %v", b, data)
	}
}
