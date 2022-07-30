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
	"io"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness/statecache"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/grpc"
)

// workerStatusHandler stores the communication information of WorkerStatus API.
type workerStatusHandler struct {
	conn             *grpc.ClientConn
	shouldShutdown   int32
	wg               sync.WaitGroup
	cache            *statecache.SideInputCache
	metStoreToString func(*strings.Builder)
}

func newWorkerStatusHandler(ctx context.Context, endpoint string, cache *statecache.SideInputCache, metStoreToString func(*strings.Builder)) (*workerStatusHandler, error) {
	sconn, err := dial(ctx, endpoint, "status", 60*time.Second)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect: %v\n", endpoint)
	}
	return &workerStatusHandler{conn: sconn, shouldShutdown: 0, cache: cache, metStoreToString: metStoreToString}, nil
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

func memoryUsage(statusInfo *strings.Builder) {
	statusInfo.WriteString("\n============Memory Usage============\n")
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	statusInfo.WriteString(fmt.Sprintf("heap in-use-spans/allocated/total/max = %d/%d/%d/%d MB\n", m.HeapInuse>>20, m.HeapAlloc>>20, m.TotalAlloc>>20, m.HeapSys>>20))
	statusInfo.WriteString(fmt.Sprintf("stack in-use-spans/max = %d/%d MB\n", m.StackInuse>>20, m.StackSys>>20))
	statusInfo.WriteString(fmt.Sprintf("GC-CPU percentage = %.2f %%\n", m.GCCPUFraction*100))
	statusInfo.WriteString(fmt.Sprintf("Last GC time: %v\n", time.Unix(0, int64(m.LastGC))))
	statusInfo.WriteString(fmt.Sprintf("Next GC: %v MB\n", m.NextGC>>20))
}

func (w *workerStatusHandler) activeProcessBundleStates(statusInfo *strings.Builder) {
	statusInfo.WriteString("\n============Active Process Bundle States============\n")
	w.metStoreToString(statusInfo)
}

func (w *workerStatusHandler) cacheStats(statusInfo *strings.Builder) {
	statusInfo.WriteString("\n============Cache Stats============\n")
	statusInfo.WriteString(fmt.Sprintf("State Cache:\n%+v\n", w.cache.CacheMetrics()))
}

func goroutineDump(statusInfo *strings.Builder) {
	statusInfo.WriteString("\n============Goroutine Dump============\n")
	profile := pprof.Lookup("goroutine")
	if profile != nil {
		profile.WriteTo(statusInfo, 1)
	}
}

func buildInfo(statusInfo *strings.Builder) {
	statusInfo.WriteString("\n============Build Info============\n")
	if info, ok := debug.ReadBuildInfo(); ok {
		statusInfo.WriteString(info.String())
	}
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

		statusInfo := &strings.Builder{}
		memoryUsage(statusInfo)
		w.activeProcessBundleStates(statusInfo)
		w.cacheStats(statusInfo)
		goroutineDump(statusInfo)
		buildInfo(statusInfo)

		response := &fnpb.WorkerStatusResponse{Id: req.GetId(), StatusInfo: statusInfo.String()}
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
