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
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
)

// workerStatusHandler stores the communication information of WorkerStatus API.
type workerStatusHandler struct {
	conn     *grpc.ClientConn
	shutdown int32
	resp     chan *fnpb.WorkerStatusResponse
}

// newWorkerStatusHandler
func newWorkerStatusHandler(ctx context.Context, endpoint string) (*workerStatusHandler, error) {
	sconn, err := dial(ctx, endpoint, 60*time.Second)
	if err != nil {
		return &workerStatusHandler{}, errors.Wrapf(err, "failed to connect: %v\n", endpoint)
	}
	return &workerStatusHandler{conn: sconn, shutdown: 0, resp: make(chan *fnpb.WorkerStatusResponse)}, nil
}

// handleRequest manages the WorkerStatus API.
func (w *workerStatusHandler) handleRequest(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	statusClient := fnpb.NewBeamFnWorkerStatusClient(w.conn)
	stub, err := statusClient.WorkerStatus(ctx)
	if err != nil {
		log.Errorf(ctx, "status client not established: %v", err)
	}
	go w.writer(ctx, stub)
	w.reader(ctx, stub)
}

// writer writes the WorkerStatusResponse recevied from the response channel.
func (w *workerStatusHandler) writer(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	for resp := range w.resp {
		log.Debugf(ctx, "RESP-status: %v", resp.GetId())
		if err := stub.Send(resp); err != nil && err != io.EOF {
			log.Errorf(ctx, "workerStatus.Writer: Failed to respond: %v", err)
		}
	}
	log.Debugf(ctx, "exiting workerStatusHandler.Writer()")
}

// reader reads the WorkerStatusRequest from the stream and sends a processed WorkerStatusResponse to
// a response channel.
func (w *workerStatusHandler) reader(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	for atomic.LoadInt32(&w.shutdown) == 0 {
		req, err := stub.Recv()
		if err != nil {
			log.Debugf(ctx, "exiting workerStatusHandler.Reader(): %v", err)
			return
		}
		log.Debugf(ctx, "RECV-status: %v", req.GetId())
		buf := make([]byte, 1<<16)
		runtime.Stack(buf, true)
		response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: string(buf)}
		if w.resp != nil {
			w.resp <- response
		} else {
			return
		}
	}
}

// close stops the reader first, closes the response channel thereby stopping writer and finally closes the gRPC connection.
func (w *workerStatusHandler) close(ctx context.Context, wg *sync.WaitGroup) {
	atomic.StoreInt32(&w.shutdown, 1)
	close(w.resp)
	wg.Wait()
	if err := w.conn.Close(); err != nil {
		log.Errorf(ctx, "error closing status endpoint connection: %v", err)
	}
}
