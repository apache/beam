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

package tools

import (
	"context"
	"fmt"
	"log"
	"net"
	"reflect"
	"sync"
	"testing"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
)

type s struct {
	A int    `json:"a,omitempty"`
	B string `json:"b,omitempty"`
	C bool   `json:"c,omitempty"`
	D *s     `json:"d,omitempty"`
}

// TestConversions verifies that we can process proto structs via JSON.
func TestConversions(t *testing.T) {
	tests := []s{
		s{},
		s{A: 2},
		s{B: "foo"},
		s{C: true},
		s{D: &s{A: 3}},
		s{A: 1, B: "bar", C: true, D: &s{A: 3, B: "baz"}},
	}

	for _, test := range tests {
		enc, err := OptionsToProto(test)
		if err != nil {
			t.Errorf("Failed to marshal %v: %v", test, err)
		}
		var ret s
		if err := ProtoToOptions(enc, &ret); err != nil {
			t.Errorf("Failed to unmarshal %v from %v: %v", test, enc, err)
		}
		if !reflect.DeepEqual(test, ret) {
			t.Errorf("Unmarshal(Marshal(%v)) = %v, want %v", test, ret, test)
		}
	}
}

type ProvisionServiceServicer struct {
	fnpb.UnimplementedProvisionServiceServer
}

func (p ProvisionServiceServicer) GetProvisionInfo(ctx context.Context, req *fnpb.GetProvisionInfoRequest) (*fnpb.GetProvisionInfoResponse, error) {
	return &fnpb.GetProvisionInfoResponse{Info: &fnpb.ProvisionInfo{RetrievalToken: "token"}}, nil
}

func setup(addr *string, wg *sync.WaitGroup) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to find an open port: %v", err)
	}
	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	*addr = fmt.Sprintf(":%d", port)

	server := grpc.NewServer()
	defer server.Stop()

	prs := &ProvisionServiceServicer{}
	fnpb.RegisterProvisionServiceServer(server, prs)

	wg.Done()

	if err := server.Serve(l); err != nil {
		log.Fatalf("cannot serve the server: %v", err)
	}
}

func TestProvisionInfo(t *testing.T) {

	endpoint := ""
	var wg sync.WaitGroup
	wg.Add(1)
	go setup(&endpoint, &wg)
	wg.Wait()

	got, err := ProvisionInfo(context.Background(), endpoint)
	if err != nil {
		t.Errorf("error in response: %v", err)
	}
	want := &fnpb.ProvisionInfo{RetrievalToken: "token"}
	if got.GetRetrievalToken() != want.GetRetrievalToken() {
		t.Errorf("provision.Info() = %v, want %v", got, want)
	}
}
