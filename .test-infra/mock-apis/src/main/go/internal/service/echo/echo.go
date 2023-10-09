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
	echoPath          = "/proto.echo.v1.EchoService/Echo"
	echoPathAlias     = "/v1/echo"
	healthPath        = "/grpc.health.v1.Health/Check"
	healthPathAlias   = "/v1/healthz"
)

var (
	defaultLogger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo")
)

// Option applies optional parameters to the echo service.
type Option interface {
	apply(srv *echo)
}

// WithLogger overrides the default logging.Logger.
func WithLogger(logger logging.Logger) Option {
	return &loggerOpt{
		v: logger,
	}
}

// WithLoggerFields WitLoggerFields supplies the echo service's logging.Logger with logging.Field slice.
func WithLoggerFields(fields ...logging.Field) Option {
	return &loggerFieldsOpts{
		v: fields,
	}
}

// WithMetricWriter supplies the echo service with a metric.Writer.
func WithMetricWriter(writer metric.Writer) Option {
	return &metricWriterOpt{
		v: writer,
	}
}

// Register a grpc.Server with the echov1.EchoService. Returns a http.Handler or error.
func Register(s *grpc.Server, decrementer cache.Decrementer, opts ...Option) (http.Handler, error) {
	srv := &echo{
		decrementer: decrementer,
		logger:      defaultLogger,
	}
	for _, opt := range opts {
		opt.apply(srv)
	}

	echov1.RegisterEchoServiceServer(s, srv)
	grpc_health_v1.RegisterHealthServer(s, srv)

	return srv, nil
}

type echo struct {
	echov1.UnimplementedEchoServiceServer
	grpc_health_v1.UnimplementedHealthServer
	decrementer   cache.Decrementer
	logger        logging.Logger
	loggerFields  []logging.Field
	metricsWriter metric.Writer
}

// ServeHTTP implements http.Handler, allowing echo to support HTTP clients in addition to gRPC.
func (srv *echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f, ok := map[string]http.HandlerFunc{
		echoPath:        srv.httpHandler,
		echoPathAlias:   srv.httpHandler,
		healthPath:      srv.checkHandler,
		healthPathAlias: srv.checkHandler,
	}[r.URL.Path]; ok {
		f(w, r)
		return
	}
	http.Error(w, fmt.Sprintf("%s not found", r.URL.Path), http.StatusNotFound)
}

// Check checks whether echo service's underlying decrementer is alive.
func (srv *echo) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if err := srv.decrementer.Alive(ctx); err != nil {
		return nil, err
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (srv *echo) checkHandler(w http.ResponseWriter, r *http.Request) {
	resp, err := srv.Check(r.Context(), nil)
	if err != nil {
		srv.logger.Error(r.Context(), err, srv.loggerFields...)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		srv.logger.Error(r.Context(), err, srv.loggerFields...)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Watch the health of the echov1.EchoServiceServer.
func (srv *echo) Watch(request *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	resp, err := srv.Check(server.Context(), request)
	if err != nil {
		srv.logger.Error(server.Context(), err, srv.loggerFields...)
		return err
	}
	return server.Send(resp)
}

// Echo a EchoRequest with a EchoResponse. Decrements an underlying quota identified by the id of the request.
// Returns a cache.IsNotExist if request's id does not map to a key in the cache.
// See cache.Refresher for how the cache refreshes the quota identified by the request id.
func (srv *echo) Echo(ctx context.Context, request *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	v, err := srv.decrementer.Decrement(ctx, request.Id)
	if cache.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "error: source not found: %s, err %v", request.Id, err)
	}
	if err != nil {
		srv.logger.Error(ctx, err, srv.loggerFields...)
		return nil, status.Errorf(codes.Internal, "error: encountered from cache for resource: %srv, err %v", request.Id, err)
	}

	if srv.metricsWriter != nil {
		if err := srv.metricsWriter.Write(ctx, path.Join(metricsNamePrefix, request.Id), "unit", &metric.Point{
			Timestamp: time.Now(),
			Value:     v + 1,
		}); err != nil {
			srv.logger.Error(ctx, err, srv.loggerFields...)
		}
	}

	if v < 0 {
		return nil, status.Errorf(codes.ResourceExhausted, "error: resource exhausted for: %srv", request.Id)
	}
	return &echov1.EchoResponse{
		Id:      request.Id,
		Payload: request.Payload,
	}, nil
}

func (srv *echo) httpHandler(w http.ResponseWriter, r *http.Request) {
	var body *echov1.EchoRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		err = fmt.Errorf("error decoding request body: %w", err)
		srv.logger.Error(r.Context(), err, srv.loggerFields...)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := srv.Echo(r.Context(), body)
	if status.Code(err) == http.StatusNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
