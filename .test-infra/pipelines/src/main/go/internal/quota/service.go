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

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/k8s"
	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/logging"
	quotav1 "github.com/apache/beam/test-infra/pipelines/src/main/go/internal/proto/quota/v1"
	"google.golang.org/grpc"
)

const (
	jobName = "refresher-service"
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
		logger: logging.NewFromEnvironment(
			context.Background(),
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
func (q *quotaService) Create(ctx context.Context, request *quotav1.CreateQuotaRequest) (*quotav1.CreateQuotaResponse, error) {
	q.logger.Debug(ctx, "received request",
		logging.Any("request", request))

	qq := request.Quota

	spec := (*k8s.Spec)(q.spec.RefresherServiceSpec)
	spec.Name = fmt.Sprintf("%s-%s", jobName, qq.Id)

	j, err := q.spec.JobsClient.Start(ctx, spec)
	if err != nil {
		q.logger.Error(ctx, err.Error(), logging.Any("request", request),
			logging.Any("jobSpec", spec))
		return nil, err
	}

	q.logger.Debug(ctx, "created refresher-service job",
		logging.Any("job", j))

	return &quotav1.CreateQuotaResponse{}, nil
}

// List receives and fulfills a request to list available quotas.
func (q *quotaService) List(ctx context.Context, request *quotav1.ListQuotasRequest) (*quotav1.ListQuotasResponse, error) {
	return nil, fmt.Errorf("not yet implemented")
}

// Delete receives and fulfills a request to delete a quota.
func (q *quotaService) Delete(ctx context.Context, request *quotav1.DeleteQuotaRequest) (*quotav1.DeleteQuotaResponse, error) {
	q.logger.Info(ctx, "received delete quota request",
		logging.Any("request", request))

	payload := []byte(fmt.Sprintf("%s:%s", request.Id, "delete"))

	if err := q.spec.Publisher.Publish(ctx, request.Id, payload); err != nil {
		return nil, fmt.Errorf("error publishing delete request key: %s, payload: %s, err %w", request.Id, string(payload), err)
	}

	q.logger.Debug(ctx, "published deletion request",
		logging.String("key", request.Id),
		logging.String("payload", string(payload)))

	return &quotav1.DeleteQuotaResponse{
		Quota: &quotav1.Quota{
			Id: request.Id,
		},
	}, nil
}

// Describe receives and fulfills a request to describe a quota.
func (q *quotaService) Describe(ctx context.Context, request *quotav1.DescribeQuotaRequest) (*quotav1.DescribeQuotaResponse, error) {
	return nil, fmt.Errorf("not yet implemented")
}
