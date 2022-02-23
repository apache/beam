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
	"runtime"
	"sync"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

type workerStatusHandler struct {
	stub     fnpb.BeamFnWorkerStatus_WorkerStatusClient
	resp     chan *fnpb.WorkerStatusResponse
	shutdown int32
}

func newWorkerStatusHandler(ctx context.Context, conn *grpc.ClientConn) (*workerStatusHandler, error) {
	statusClient := fnpb.NewBeamFnWorkerStatusClient(conn)
	stub, err := statusClient.WorkerStatus(ctx)
	if err != nil {
		return &workerStatusHandler{}, err
	}
	return &workerStatusHandler{stub: stub, resp: make(chan *fnpb.WorkerStatusResponse, 100)}, nil
}

func (w *workerStatusHandler) Writer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for res := range w.resp {
		log.Debugf(ctx, "RESP-status: %v", proto.MarshalTextString(res))

		if err := w.stub.Send(res); err != nil {
			log.Errorf(ctx, "workerStatus.Writer: Failed to respond: %v", err)
		}
	}
	log.Debugf(ctx, "status response channel closed")
}

func (w *workerStatusHandler) Reader(ctx context.Context, wg *sync.WaitGroup) {
	for {
		req, err := w.stub.Recv()
		if err != nil {
			close(w.resp)
			wg.Wait()
			return
		}
		log.Debugf(ctx, "RECV-status: %v", proto.MarshalTextString(req))
		go w.handleRequest(ctx, req)
	}
}

func (w *workerStatusHandler) handleRequest(ctx context.Context, req *fnpb.WorkerStatusRequest) {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: string(buf)}
	// if atomic.LoadInt32(&w.shutdown) == 0 {
	if w.resp != nil {
		w.resp <- response
	}
	// }
}
