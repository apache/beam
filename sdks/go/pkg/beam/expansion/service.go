package expansion

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"reflect"
)

type Expander interface {
	Uri() string
	DoFn() reflect.Type
	Expand(s beam.Scope)
	Config() reflect.Type
}

func Register(g *grpc.Server, expander Expander) error {
	svc := &expansionService{
		configs: make(map[string]*jobmanagement_v1.SchemaTransformConfig),
	}
	if err := svc.register(expander); err != nil {
		return err
	}
	jobmanagement_v1.RegisterExpansionServiceServer(g, svc)
	healthgrpc.RegisterHealthServer(g, svc)

	return nil
}

var _ jobmanagement_v1.ExpansionServiceServer = &expansionService{}

type expansionService struct {
	configs     map[string]*jobmanagement_v1.SchemaTransformConfig
	expansion   *jobmanagement_v1.ExpansionResponse
	healthcheck *health.Server

	jobmanagement_v1.UnimplementedExpansionServiceServer
	healthgrpc.UnimplementedHealthServer
}

func (svc *expansionService) Check(_ context.Context, _ *healthgrpc.HealthCheckRequest) (*healthgrpc.HealthCheckResponse, error) {
	status := healthgrpc.HealthCheckResponse_NOT_SERVING
	if svc.configs != nil && svc.expansion != nil {
		status = healthgrpc.HealthCheckResponse_SERVING
	}
	return &healthgrpc.HealthCheckResponse{
		Status: status,
	}, nil
}

func (svc *expansionService) Watch(req *healthgrpc.HealthCheckRequest, server healthgrpc.Health_WatchServer) error {
	resp, err := svc.Check(server.Context(), req)
	if err != nil {
		return err
	}
	return server.Send(resp)
}

func (svc *expansionService) Expand(_ context.Context, _ *jobmanagement_v1.ExpansionRequest) (*jobmanagement_v1.ExpansionResponse, error) {
	return svc.expansion, nil
}

func (svc *expansionService) DiscoverSchemaTransform(_ context.Context, _ *jobmanagement_v1.DiscoverSchemaTransformRequest) (*jobmanagement_v1.DiscoverSchemaTransformResponse, error) {
	return &jobmanagement_v1.DiscoverSchemaTransformResponse{
		SchemaTransformConfigs: svc.configs,
	}, nil
}

func (svc *expansionService) register(exp Expander) error {
	t := exp.DoFn()
	name := fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
	if svc.configs == nil {
		svc.configs = make(map[string]*jobmanagement_v1.SchemaTransformConfig)
	}

	p, s := beam.NewPipelineWithRoot()
	exp.Expand(s)

	edges, _, err := p.Build()
	if err != nil {
		return err
	}

	var edge *graph.MultiEdge

	for _, e := range edges {
		if e.Name() == name {
			edge = e
		}
	}

	edges = []*graph.MultiEdge{edge}

	pbpip, err := graphx.Marshal(edges, &graphx.Options{})
	if err != nil {
		return err
	}

	if len(pbpip.RootTransformIds) != 1 {
		return fmt.Errorf("error registering %T, expect a single RootTransformId, got: %s", exp, pbpip.RootTransformIds)
	}

	transformId := pbpip.RootTransformIds[0]

	sch, err := schema.FromType(exp.Config())
	if err != nil {
		return err
	}

	svc.configs[exp.Uri()] = &jobmanagement_v1.SchemaTransformConfig{
		ConfigSchema: sch,
	}
	svc.expansion = &jobmanagement_v1.ExpansionResponse{
		Components:   pbpip.Components,
		Transform:    pbpip.Components.Transforms[transformId],
		Requirements: pbpip.Requirements,
	}

	return nil
}
