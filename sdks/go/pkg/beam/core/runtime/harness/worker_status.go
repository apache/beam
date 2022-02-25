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
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

type workerStatusHandler struct {
	conn     *grpc.ClientConn
	shutdown int32
	resp     chan *fnpb.WorkerStatusResponse
}

func newWorkerStatusHandler(ctx context.Context, endpoint string) (*workerStatusHandler, error) {
	sconn, err := dial(ctx, endpoint, 60*time.Second)
	if err != nil {
		return &workerStatusHandler{}, errors.Wrapf(err, "failed to connect: %v\n", endpoint)
	}
	return &workerStatusHandler{conn: sconn, shutdown: 0, resp: make(chan *fnpb.WorkerStatusResponse, 100)}, nil
}

func (w *workerStatusHandler) handleRequest(ctx context.Context) {
	statusClient := fnpb.NewBeamFnWorkerStatusClient(w.conn)

	for atomic.LoadInt32(&w.shutdown) == 0 {
		stub, err := statusClient.WorkerStatus(ctx)
		if err != nil {
			log.Errorf(ctx, "status client not established: %v", err)
		}

		go w.Writer(ctx, stub)
		w.Reader(ctx, stub)
	}
}

func (w *workerStatusHandler) Writer(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	for res := range w.resp {
		log.Debugf(ctx, "RESP-status: %v", proto.MarshalTextString(res))

		if err := stub.Send(res); err != nil {
			log.Errorf(ctx, "workerStatus.Writer: Failed to respond: %v", err)
		}
	}
	log.Debugf(ctx, "status response channel closed")
}

func (w *workerStatusHandler) Reader(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	req, err := stub.Recv()
	if err != nil {
		close(w.resp)
		return
	}
	log.Debugf(ctx, "RECV-status: %v", proto.MarshalTextString(req))
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: string(buf)}
	if atomic.LoadInt32(&w.shutdown) == 0 {
		if w.resp != nil {
			w.resp <- response
		}
	} else {
		close(w.resp)
	}
}

func (w *workerStatusHandler) close(ctx context.Context) {
	atomic.StoreInt32(&w.shutdown, 1)
	if err := w.conn.Close(); err != nil {
		log.Errorf(ctx, "error closing status endpoint connection: %v", err)
	}
}

// func (w *workerStatusHandler) handleRequest(ctx context.Context, req *fnpb.WorkerStatusRequest) {
// 	buf := make([]byte, 1<<16)
// 	runtime.Stack(buf, true)
// 	response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: string(buf)}
// 	// if atomic.LoadInt32(&w.shutdown) == 0 {
// 	if w.resp != nil {
// 		w.resp <- response
// 	}
// 	// }
// }
