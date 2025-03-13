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

// Package echo contains the EchoService API implementation.
package echo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"reflect"
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
	PathAlias         = "/v1/echo"
	healthPath        = "/grpc.health.v1.Health/Check"
	healthPathAlias   = "/v1/healthz"
)

type Options struct {
	Decrementer   cache.Decrementer
	MetricsWriter metric.Writer
	Logger        *slog.Logger
	LoggingAttrs  []slog.Attr
}

// Register a grpc.Server with the echov1.EchoService. Returns a http.Handler or error.
func Register(s *grpc.Server, opts *Options) (http.Handler, error) {
	if opts.Logger == nil {
		opts.Logger = logging.New(&logging.Options{
			Name: reflect.TypeOf((*echo)(nil)).PkgPath(),
		})
	}
	var attrs []any
	for _, attr := range opts.LoggingAttrs {
		attrs = append(attrs, attr)
	}
	opts.Logger = opts.Logger.With(attrs...)
	srv := &echo{
		opts: opts,
	}

	echov1.RegisterEchoServiceServer(s, srv)
	grpc_health_v1.RegisterHealthServer(s, srv)

	return srv, nil
}

type echo struct {
	echov1.UnimplementedEchoServiceServer
	grpc_health_v1.UnimplementedHealthServer
	opts *Options
}

// ServeHTTP implements http.Handler, allowing echo to support HTTP clients in addition to gRPC.
func (srv *echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case echoPath, PathAlias:
		srv.httpHandler(w, r)
	case healthPath, healthPathAlias:
		srv.checkHandler(w, r)
	default:
		http.Error(w, fmt.Sprintf("%s not found", r.URL.Path), http.StatusNotFound)
	}
}

// Check checks whether echo service's underlying decrementer is alive.
func (srv *echo) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if err := srv.opts.Decrementer.Alive(ctx); err != nil {
		return nil, err
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (srv *echo) checkHandler(w http.ResponseWriter, r *http.Request) {
	resp, err := srv.Check(r.Context(), nil)
	if err != nil {

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		srv.opts.Logger.Log(r.Context(), slog.LevelError, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Watch the health of the echov1.EchoServiceServer.
func (srv *echo) Watch(request *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	resp, err := srv.Check(server.Context(), request)
	if err != nil {
		srv.opts.Logger.Log(server.Context(), slog.LevelError, err.Error())
		return err
	}
	return server.Send(resp)
}

// Echo a EchoRequest with a EchoResponse. Decrements an underlying quota identified by the id of the request.
// Returns a cache.IsNotExist if request's id does not map to a key in the cache.
// See cache.Refresher for how the cache refreshes the quota identified by the request id.
func (srv *echo) Echo(ctx context.Context, request *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	v, err := srv.opts.Decrementer.Decrement(ctx, request.Id)
	if cache.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "error: source not found: %s, err %v", request.Id, err)
	}
	if err != nil {
		srv.opts.Logger.Log(ctx, slog.LevelError, err.Error())
		return nil, status.Errorf(codes.Internal, "error: encountered from cache for resource: %srv, err %v", request.Id, err)
	}

	if err := srv.writeMetric(ctx, request.Id, v); err != nil {
		return nil, err
	}

	if v < 0 {
		return nil, status.Errorf(codes.ResourceExhausted, "error: resource exhausted for: %s", request.Id)
	}

	return &echov1.EchoResponse{
		Id:      request.Id,
		Payload: request.Payload,
	}, nil
}

func (srv *echo) writeMetric(ctx context.Context, id string, value int64) error {
	if srv.opts.MetricsWriter == nil {
		return nil
	}
	if err := srv.opts.MetricsWriter.Write(ctx, path.Join(metricsNamePrefix, id), "unit", &metric.Point{
		Timestamp: time.Now(),
		Value:     value + 1,
	}); err != nil {
		srv.opts.Logger.Log(ctx, slog.LevelError, err.Error())
	}
	return nil
}

func (srv *echo) httpHandler(w http.ResponseWriter, r *http.Request) {
	var body *echov1.EchoRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		err = fmt.Errorf("error decoding request body, payload field of %T needs to be base64 encoded, error: %w", body, err)
		srv.opts.Logger.Log(r.Context(), slog.LevelError, err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := srv.Echo(r.Context(), body)

	switch status.Code(err) {
	case codes.OK:
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			srv.opts.Logger.Log(r.Context(), slog.LevelError, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case codes.InvalidArgument:
		http.Error(w, err.Error(), http.StatusBadRequest)
	case codes.DeadlineExceeded:
		http.Error(w, err.Error(), http.StatusRequestTimeout)
	case codes.NotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case codes.ResourceExhausted:
		http.Error(w, err.Error(), http.StatusTooManyRequests)
	default:
		srv.opts.Logger.Log(r.Context(), slog.LevelError, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
