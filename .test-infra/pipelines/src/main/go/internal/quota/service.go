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

// Package quota manages a cached quota.
package quota

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/k8s"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	quotav1 "github.com/apache/beam/test-infra/pipelines/src/main/go/internal/proto/quota/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	jobNamePrefix = "refresher-service"
)

// RefresherServiceSpec configures a Kubernetes Job to refresh a quota.
type RefresherServiceSpec k8s.Spec

func (spec *RefresherServiceSpec) isValid() error {
	if spec.Image == "" {
		return fmt.Errorf("%T.Image property is required", spec)
	}
	return nil
}

// ServiceSpec configures a quota service.
type ServiceSpec struct {
	RefresherServiceSpec *RefresherServiceSpec
	Cache                cache.Decrementer
	Publisher            cache.Publisher
	JobsClient           *k8s.Jobs
}

func (spec *ServiceSpec) isValid() error {
	for _, prop := range []interface{}{
		spec.RefresherServiceSpec,
		spec.Cache,
		spec.Publisher,
		spec.JobsClient,
	} {
		if prop == nil {
			return fmt.Errorf("%T is required but nil", prop)
		}
	}
	return spec.RefresherServiceSpec.isValid()
}

// RegisterService registers a quota service to a grpc.Service using a
// ServiceSpec.
func RegisterService(ctx context.Context, server *grpc.Server, spec *ServiceSpec) error {
	if spec.RefresherServiceSpec.Labels == nil {
		spec.RefresherServiceSpec.Labels = map[string]string{}
	}
	if err := spec.isValid(); err != nil {
		return err
	}
	svc := &quotaService{
		logger: logging.New(
			ctx,
			"github.com/apache/beam/.test-infra/pipelines/src/main/go/internal/quota",
			logging.LevelVariable),
		spec: spec,
	}

	quotav1.RegisterQuotaServiceServer(server, svc)

	return nil
}

type quotaService struct {
	quotav1.UnimplementedQuotaServiceServer
	logger *logging.Logger
	spec   *ServiceSpec
}

// Create receives and fulfills a request to create a quota.
func (q *quotaService) Create(ctx context.Context, request *quotav1.CreateRequest) (*quotav1.CreateResponse, error) {
	q.logger.Debug(ctx, "received request",
		logging.Any("request", request))

	qq := request.Quota
	if qq == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request %T is nil but required", request.Quota)
	}

	spec := (*k8s.Spec)(q.spec.RefresherServiceSpec)
	spec.Name = jobName(qq.Id)
	j, err := q.spec.JobsClient.Create(ctx, spec,
		cache.Host.Key(), cache.Host.Value(),
		cache.QuotaId.Key(), request.Quota.Id,
		cache.QuotaSize.Key(), fmt.Sprint(request.Quota.Size),
		cache.QuotaRefreshInterval.Key(), fmt.Sprintf("%vms", request.Quota.RefreshMillisecondsInterval),
		logging.LevelVariable.Key(), logging.LevelVariable.Value())

	if err != nil {
		errorId := uuid.New().String()
		q.logger.Error(ctx, err, logging.Any("request", request),
			logging.Any("jobSpec", spec), logging.String("errorId", errorId))
		return nil, status.Errorf(codes.Internal, "Service encountered an internal error associated with the request, search logs for errorId: %s", errorId)
	}

	q.logger.Debug(ctx, "created refresher-service job",
		logging.Any("job", j))

	return &quotav1.CreateResponse{
		Quota: &quotav1.Quota{
			Id:   qq.Id,
			Size: qq.Size,
		},
	}, nil
}

// List receives and fulfills a request to list available quotas.
func (q *quotaService) List(ctx context.Context, _ *quotav1.ListRequest) (*quotav1.ListResponse, error) {
	var result []*quotav1.Quota
	jobs, err := q.spec.JobsClient.List(ctx)
	if err != nil {
		errorId := uuid.New().String()
		q.logger.Error(ctx, err, logging.String("errorId", errorId))
		return nil, status.Errorf(codes.Internal, "Service encountered an internal error associated with the request, search logs for errorId: %s", errorId)
	}
	for _, k := range jobs {
		result = append(result, &quotav1.Quota{
			Id: strings.ReplaceAll(k.Name, fmt.Sprintf("%s-", jobNamePrefix), ""),
		})
	}
	return &quotav1.ListResponse{
		List: result,
	}, nil
}

// Delete receives and fulfills a request to delete a quota.
func (q *quotaService) Delete(ctx context.Context, request *quotav1.DeleteRequest) (*quotav1.DeleteResponse, error) {
	q.logger.Debug(ctx, "received delete quota request",
		logging.Any("request", request))

	if request.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "missing required Id from request")
	}

	payload := []byte(fmt.Sprintf("%s:%s", request.Id, "delete"))

	if err := q.spec.Publisher.Publish(ctx, request.Id, payload); err != nil {
		errorId := uuid.New().String()
		q.logger.Error(ctx, fmt.Errorf("error publishing delete request: %w", err),
			logging.String("key", request.Id),
			logging.String("errorId", errorId),
			logging.String("payload", string(payload)),
		)
		return nil, status.Errorf(codes.Internal, "Service encountered internal error associated with request, search logs for associated errorId: %s", errorId)
	}

	q.logger.Debug(ctx, "published deletion request",
		logging.String("key", request.Id),
		logging.String("payload", string(payload)))

	if err := q.spec.JobsClient.Delete(ctx, jobName(request.Id)); err != nil {
		errorId := uuid.New().String()
		q.logger.Error(ctx, fmt.Errorf("error deleting Job: %w", err),
			logging.Any("request", request),
			logging.String("errorId", errorId))

		return nil, status.Errorf(codes.Internal, "Service encountered internal error associated with request, search logs for associated errorId: %s", errorId)
	}

	return &quotav1.DeleteResponse{
		Quota: &quotav1.Quota{
			Id: request.Id,
		},
	}, nil
}

// Describe receives and fulfills a request to describe a quota.
func (q *quotaService) Describe(ctx context.Context, request *quotav1.DescribeRequest) (*quotav1.DescribeResponse, error) {
	name := jobName(request.Id)
	job, err := q.spec.JobsClient.Describe(ctx, name)
	if err != nil {
		errorId := uuid.New().String()
		q.logger.Error(ctx, fmt.Errorf("error querying Job: %w", err),
			logging.Any("request", request),
			logging.String("errorId", errorId))

		return nil, status.Errorf(codes.Internal, "Service encountered internal error associated with request, search logs for associated errorId: %s", errorId)
	}
	return &quotav1.DescribeResponse{
		Quota: &quotav1.Quota{
			Id: strings.ReplaceAll(job.Name, fmt.Sprintf("%s-", jobNamePrefix), ""),
		},
	}, nil
}

func jobName(quotaId string) string {
	return fmt.Sprintf("%s-%s", jobNamePrefix, quotaId)
}
