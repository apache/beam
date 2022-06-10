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
	conn           *grpc.ClientConn
	shouldShutdown int32
	wg             sync.WaitGroup
}

func newWorkerStatusHandler(ctx context.Context, endpoint string) (*workerStatusHandler, error) {
	sconn, err := dial(ctx, endpoint, 60*time.Second)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect: %v\n", endpoint)
	}
	return &workerStatusHandler{conn: sconn, shouldShutdown: 0}, nil
}

func (w *workerStatusHandler) isAlive() bool {
	return atomic.LoadInt32(&w.shouldShutdown) == 0
}

func (w *workerStatusHandler) shutdown() {
	atomic.StoreInt32(&w.shouldShutdown, 1)
}

// start starts the reader to accept WorkerStatusRequest and send WorkerStatusResponse with WorkerStatus API.
func (w *workerStatusHandler) start(ctx context.Context) error {
	statusClient := fnpb.NewBeamFnWorkerStatusClient(w.conn)
	stub, err := statusClient.WorkerStatus(ctx)
	if err != nil {
		log.Errorf(ctx, "status client not established: %v", err)
		return errors.WithContext(err, "status endpoint client not established")
	}
	w.wg.Add(1)
	go w.reader(ctx, stub)
	return nil
}

// reader reads the WorkerStatusRequest from the stream and sends a processed WorkerStatusResponse to
// a response channel.
func (w *workerStatusHandler) reader(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	defer w.wg.Done()
	buf := make([]byte, 1<<16)
	for w.isAlive() {
		req, err := stub.Recv()
		if err != nil && err != io.EOF {
			log.Debugf(ctx, "exiting workerStatusHandler.Reader(): %v", err)
			return
		}
		log.Debugf(ctx, "RECV-status: %v", req.GetId())
		runtime.Stack(buf, true)
		response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: string(buf)}
		if err := stub.Send(response); err != nil && err != io.EOF {
			log.Errorf(ctx, "workerStatus.Writer: Failed to respond: %v", err)
		}
	}
}

// stop stops the reader and closes worker status endpoint connection with the runner.
func (w *workerStatusHandler) stop(ctx context.Context) error {
	w.shutdown()
	w.wg.Wait()
	if err := w.conn.Close(); err != nil {
		log.Errorf(ctx, "error closing status endpoint connection: %v", err)
		return errors.WithContext(err, "error closing status endpoint connection")
	}
	return nil
}
