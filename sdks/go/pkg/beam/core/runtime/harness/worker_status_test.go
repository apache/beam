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
package harness

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
)

type BeamFnWorkerStatusServicer struct {
	fnpb.UnimplementedBeamFnWorkerStatusServer

	response   chan string
	numRequest int
}

func (w BeamFnWorkerStatusServicer) mustEmbedUnimplementedBeamFnWorkerStatusServer() {

}

func (w BeamFnWorkerStatusServicer) WorkerStatus(b fnpb.BeamFnWorkerStatus_WorkerStatusServer) error {
	b.Send(&fnpb.WorkerStatusRequest{Id: "1"})
	resp, err := b.Recv()
	if err != nil {
		return fmt.Errorf("error receiving response b.recv: %v", err)
	}
	w.response <- resp.GetStatusInfo()
	return nil
}

type serverData struct {
	server *grpc.Server
}

func (s *serverData) setUp(port string, srv *BeamFnWorkerStatusServicer) {
	l, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen on port 9000 %v", err)
	}

	fnpb.RegisterBeamFnWorkerStatusServer(s.server, srv)
	if err := s.server.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func TestSendStatusResponse(t *testing.T) {
	server := &serverData{server: grpc.NewServer()}
	srv := BeamFnWorkerStatusServicer{response: make(chan string)}
	go server.setUp("9000", &srv)
	defer server.server.Stop()
	ctx := context.Background()
	statusHandler, err := newWorkerStatusHandler(ctx, "localhost:9000")
	if err != nil {
		log.Fatalf("unable to create status handler: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go statusHandler.handleRequest(ctx, &wg)
	defer statusHandler.close(ctx, &wg)
	response := []string{}
	response = append(response, <-srv.response)
	if len(response) == 0 {
		t.Errorf("error in response: %v", response)
	}
}
