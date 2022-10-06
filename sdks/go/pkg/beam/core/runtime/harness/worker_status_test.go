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
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/statecache"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type BeamFnWorkerStatusServicer struct {
	fnpb.UnimplementedBeamFnWorkerStatusServer
	response chan string
}

func (w *BeamFnWorkerStatusServicer) WorkerStatus(b fnpb.BeamFnWorkerStatus_WorkerStatusServer) error {
	b.Send(&fnpb.WorkerStatusRequest{Id: "1"})
	resp, err := b.Recv()
	if err != nil {
		return fmt.Errorf("error receiving response b.recv: %v", err)
	}
	w.response <- resp.GetStatusInfo()
	return nil
}

var lis *bufconn.Listener

func setup(t *testing.T, srv *BeamFnWorkerStatusServicer) {
	const buffsize = 1024 * 1024
	server := grpc.NewServer()
	lis = bufconn.Listen(buffsize)
	fnpb.RegisterBeamFnWorkerStatusServer(server, srv)
	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
	})
}

func dialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestSendStatusResponse(t *testing.T) {
	ctx := context.Background()
	srv := &BeamFnWorkerStatusServicer{response: make(chan string)}
	setup(t, srv)

	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		t.Fatalf("unable to start test server: %v", err)
	}

	statusHandler := workerStatusHandler{conn: conn, cache: &statecache.SideInputCache{}, metStoreToString: func(builder *strings.Builder) {
		builder.WriteString("metStore metadata")
	}}
	if err := statusHandler.start(ctx); err != nil {
		t.Fatal(err)
	}

	response := []string{}
	response = append(response, <-srv.response)
	if len(response) == 0 {
		t.Errorf("no response received: %v", response)
	}

	if err := statusHandler.stop(ctx); err != nil {
		t.Error(err)
	}
}
