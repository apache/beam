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
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

func TestDeterministicEncoding(t *testing.T) {
	fields := make(map[string]*structpb.Value)
	for i := 0; i < 32; i++ {
		fields[fmt.Sprintf("key_%02d", i)] = structpb.NewNumberValue(float64(i))
	}
	msg := &structpb.Struct{Fields: fields}

	wantBytes := MustEncode(msg)
	wantBase64, err := EncodeBase64(msg)
	if err != nil {
		t.Fatalf("EncodeBase64 failed: %v", err)
	}
	wantAny, err := PackProto(msg)
	if err != nil {
		t.Fatalf("PackProto failed: %v", err)
	}

	for i := 0; i < 50; i++ {
		if got := MustEncode(msg); !bytes.Equal(got, wantBytes) {
			t.Fatalf("MustEncode produced non-deterministic bytes on iteration %d", i)
		}
		gotBase64, err := EncodeBase64(msg)
		if err != nil {
			t.Fatalf("EncodeBase64 failed: %v", err)
		}
		if gotBase64 != wantBase64 {
			t.Fatalf("EncodeBase64 produced non-deterministic output on iteration %d", i)
		}
		gotAny, err := PackProto(msg)
		if err != nil {
			t.Fatalf("PackProto failed: %v", err)
		}
		if !bytes.Equal(gotAny.GetValue(), wantAny.GetValue()) {
			t.Fatalf("PackProto produced non-deterministic bytes on iteration %d", i)
		}
	}
}
