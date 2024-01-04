package cmd

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/environment"
	"github.com/apache/beam/sdks/v2/go/cmd/wasmx/internal/udf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"log"
	"net"
	"os"
	"os/signal"
)

var (
	port    environment.Variable = "PORT"
	address string

	serveCmd = &cobra.Command{
		Use:     "serve",
		Short:   "Serve the wasmx expansion service",
		PreRunE: servePreE,
		RunE:    serveE,
	}
)

func init() {
	port.MustDefault("8097")
}

func servePreE(_ *cobra.Command, _ []string) error {
	if err := environment.Missing(port); err != nil {
		return err
	}
	p, err := port.Int()
	address = fmt.Sprintf(":%v", p)
	return err
}

func serveE(cmd *cobra.Command, args []string) error {
	g := grpc.NewServer()
	ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt)

	defer g.GracefulStop()
	defer cancel()

	errChan := make(chan error)
	log.Printf("starting wasmx expansion service at %s", address)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	svc := &service{}
	jobmanagement_v1.RegisterExpansionServiceServer(g, svc)
	healthgrpc.RegisterHealthServer(g, svc)

	go func() {
		if err := g.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
}

type service struct {
	jobmanagement_v1.UnimplementedExpansionServiceServer
	healthgrpc.UnimplementedHealthServer
}

func (svc *service) Check(ctx context.Context, request *healthgrpc.HealthCheckRequest) (*healthgrpc.HealthCheckResponse, error) {
	return &healthgrpc.HealthCheckResponse{
		Status: healthgrpc.HealthCheckResponse_SERVING,
	}, nil
}

func (svc *service) Watch(request *healthgrpc.HealthCheckRequest, server healthgrpc.Health_WatchServer) error {
	resp, err := svc.Check(server.Context(), request)
	if err != nil {
		return err
	}
	return server.Send(resp)
}

func (svc *service) Expand(_ context.Context, _ *jobmanagement_v1.ExpansionRequest) (*jobmanagement_v1.ExpansionResponse, error) {
	transform := &pipeline_v1.PTransform{
		UniqueName:    "add.wasm",
		Spec:          udf.AddWasm.FunctionSpec(),
		Inputs:        map[string]string{"i0": "n1"},
		Outputs:       map[string]string{"i0": "n2"},
		DisplayData:   nil,
		EnvironmentId: "wasm",
	}
	return &jobmanagement_v1.ExpansionResponse{
		Components: &pipeline_v1.Components{
			Transforms: map[string]*pipeline_v1.PTransform{
				"e1": transform,
			},
			Pcollections: map[string]*pipeline_v1.PCollection{
				"n1": {
					UniqueName:          "n1",
					CoderId:             "c0",
					IsBounded:           pipeline_v1.IsBounded_BOUNDED,
					WindowingStrategyId: "w0",
				},
				"n2": {
					UniqueName:          "n2",
					CoderId:             "c0",
					IsBounded:           pipeline_v1.IsBounded_BOUNDED,
					WindowingStrategyId: "w0",
				},
			},
			Coders: map[string]*pipeline_v1.Coder{
				"c0": {
					Spec: &pipeline_v1.FunctionSpec{
						Urn: "beam:coder:varint:v1",
					},
				},
				"c1": {
					Spec: &pipeline_v1.FunctionSpec{
						Urn: "beam:coder:global_window:v1",
					},
				},
			},
			WindowingStrategies: map[string]*pipeline_v1.WindowingStrategy{
				"w0": {
					WindowFn: &pipeline_v1.FunctionSpec{
						Urn: "beam:window_fn:global_windows:v1",
					},
					MergeStatus:   pipeline_v1.MergeStatus_NON_MERGING,
					WindowCoderId: "c1",
					Trigger: &pipeline_v1.Trigger{
						Trigger: &pipeline_v1.Trigger_Default_{
							Default: &pipeline_v1.Trigger_Default{},
						},
					},
					AccumulationMode: pipeline_v1.AccumulationMode_DISCARDING,
					OutputTime:       pipeline_v1.OutputTime_END_OF_WINDOW,
					ClosingBehavior:  pipeline_v1.ClosingBehavior_EMIT_IF_NONEMPTY,
					OnTimeBehavior:   pipeline_v1.OnTimeBehavior_FIRE_IF_NONEMPTY,
					EnvironmentId:    "wasm",
				},
			},
		},
		Transform: transform,
	}, nil
}

func (svc *service) DiscoverSchemaTransform(_ context.Context, _ *jobmanagement_v1.DiscoverSchemaTransformRequest) (*jobmanagement_v1.DiscoverSchemaTransformResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
