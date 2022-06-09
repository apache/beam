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
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/statecache"
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
	metStore       map[instructionID]*metrics.Store //*metrics.Store for active bundles
	cache          *statecache.SideInputCache
}

func newWorkerStatusHandler(ctx context.Context, endpoint string, metStore map[instructionID]*metrics.Store, cache *statecache.SideInputCache) (*workerStatusHandler, error) {
	sconn, err := dial(ctx, endpoint, 60*time.Second)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect: %v\n", endpoint)
	}
	return &workerStatusHandler{conn: sconn, shouldShutdown: 0, metStore: metStore, cache: cache}, nil
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

func memoryUsage() string {
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("\n Total Alloc: %v bytes \n Sys: %v bytes \n Mallocs: %v\n Frees: %v\n HeapAlloc: %v bytes", m.TotalAlloc, m.Sys, m.Mallocs, m.Frees, m.HeapAlloc)
}

func (w *workerStatusHandler) activeProcessBundleStates() string {
	var states string
	for bundleID, store := range w.metStore {
		execStates := ""
		for bundleID, state := range store.StateRegistry() {
			execStates += fmt.Sprintf("ID: %v Execution States: %#v,", bundleID, *state)

		}
		states += fmt.Sprintf("\nBundle ID: %v\nBundle State: %#v\nBundle Execution States: %v\n", bundleID, *store.BundleState(), execStates)
	}
	return states
}

func (w *workerStatusHandler) cacheStats() string {
	return fmt.Sprintf("Cache:\n%v", w.cache.CacheMetrics())
}

func goroutineDump() string {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	return string(buf)
}

// reader reads the WorkerStatusRequest from the stream and sends a processed WorkerStatusResponse to
// a response channel.
func (w *workerStatusHandler) reader(ctx context.Context, stub fnpb.BeamFnWorkerStatus_WorkerStatusClient) {
	defer w.wg.Done()

	for w.isAlive() {
		req, err := stub.Recv()
		if err != nil && err != io.EOF {
			log.Debugf(ctx, "exiting workerStatusHandler.Reader(): %v", err)
			return
		}
		log.Debugf(ctx, "RECV-status: %v", req.GetId())

		statusInfo := fmt.Sprintf("\n============Memory Usage============\n%s\n============Active Process Bundle States============\n%s\n============Cache Stats============\n%s\n============Goroutine Dump============\n%s\n", memoryUsage(), w.activeProcessBundleStates(), w.cacheStats(), goroutineDump())
		response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: statusInfo}
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
