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

package echo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/metric"
	echov1 "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/proto/echo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	metricsNamePrefix = "echo"
	httpPath          = "proto.echo.v1.EchoService/Echo"
)

var (
	defaultLogger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo")
)

type Option interface {
	apply(srv *echo)
}

func WithLogger(logger logging.Logger) Option {
	return &loggerOpt{
		v: logger,
	}
}

func WithLoggerFields(fields ...logging.Field) Option {
	return &loggerFieldsOpts{
		v: fields,
	}
}

func WithMetricWriter(writer metric.Writer) Option {
	return &metricWriterOpt{
		v: writer,
	}
}

func RegisterGrpc(s *grpc.Server, decrementer cache.Decrementer, opts ...Option) error {
	srv := &echo{
		decrementer: decrementer,
		logger:      defaultLogger,
	}
	for _, opt := range opts {
		opt.apply(srv)
	}

	echov1.RegisterEchoServiceServer(s, srv)
	grpc_health_v1.RegisterHealthServer(s, srv)

	return nil
}

func RegisterHttp(conn *grpc.ClientConn) http.Handler {
	client := echov1.NewEchoServiceClient(conn)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != httpPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req *echov1.EchoRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp, err := client.Echo(r.Context(), req)
		if err == nil {
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		if cache.IsNotExist(err) {
			http.NotFound(w, r)
			fmt.Fprint(w, err)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, err)
	})
}

type echo struct {
	echov1.UnimplementedEchoServiceServer
	grpc_health_v1.UnimplementedHealthServer
	decrementer   cache.Decrementer
	logger        logging.Logger
	loggerFields  []logging.Field
	metricsWriter metric.Writer
}

func (srv *echo) Check(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if err := srv.decrementer.Alive(ctx); err != nil {
		return nil, err
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (srv *echo) Watch(request *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	resp, err := srv.Check(server.Context(), request)
	if err != nil {
		return err
	}
	return server.Send(resp)
}

func (srv *echo) Echo(ctx context.Context, request *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	v, err := srv.decrementer.Decrement(ctx, request.Id)
	if cache.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "error: source not found: %s, err %v", request.Id, err)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error: encountered from cache for resource: %srv, err %v", request.Id, err)
	}

	if srv.metricsWriter != nil {
		srv.metricsWriter.Write(ctx, path.Join(metricsNamePrefix, request.Id), "unit", &metric.Point{
			Timestamp: time.Now(),
			Value:     v + 1,
		})
	}
	if v < 0 {
		return nil, status.Errorf(codes.ResourceExhausted, "error: resource exhausted for: %srv", request.Id)
	}
	return &echov1.EchoResponse{
		Id:      request.Id,
		Payload: request.Payload,
	}, nil
}

type loggerOpt struct {
	v logging.Logger
}

func (opt *loggerOpt) apply(svc *echo) {
	svc.logger = opt.v
}

type loggerFieldsOpts struct {
	v []logging.Field
}

func (opt *loggerFieldsOpts) apply(svc *echo) {
	svc.loggerFields = opt.v
}

type metricWriterOpt struct {
	v metric.Writer
}

func (opt *metricWriterOpt) apply(svc *echo) {
	svc.metricsWriter = opt.v
}
