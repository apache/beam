package expansion

import (
	"context"
	"encoding/base64"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/net/nettest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"reflect"
	"testing"
	"time"
)

func init() {
	beam.RegisterDoFn(reflect.TypeOf((*doFn)(nil)).Elem())
}

func TestExpansionService_DiscoverSchemaTransform(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := grpc.NewServer()
	defer g.GracefulStop()

	errChan := make(chan error)
	addrChan := make(chan string)

	if err := Register(g, &doFnX{}); err != nil {
		t.Fatal(err)
	}

	lis, err := nettest.NewLocalListener("tcp")
	if err != nil {
		t.Fatal(err)
	}

	go ready(ctx, lis, time.Second*3, addrChan, t)

	go func() {
		if err := g.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case addr := <-addrChan:
		testDiscoverSchemaTransform(t, addr, cancel)
	case err := <-errChan:
		t.Fatal(err)
		return
	case <-ctx.Done():
		return
	}
}

func TestExpansionService_Expand(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := grpc.NewServer()
	defer g.GracefulStop()

	errChan := make(chan error)
	addrChan := make(chan string)

	if err := Register(g, &doFnX{}); err != nil {
		t.Fatal(err)
	}

	lis, err := nettest.NewLocalListener("tcp")
	if err != nil {
		t.Fatal(err)
	}

	go ready(ctx, lis, time.Second*3, addrChan, t)

	go func() {
		if err := g.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case addr := <-addrChan:
		testExpand(t, addr, cancel)
	case err := <-errChan:
		t.Fatal(err)
		return
	case <-ctx.Done():
		return
	}
}

func ready(ctx context.Context, lis net.Listener, timeout time.Duration, done chan string, t *testing.T) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	healthCheck := healthgrpc.NewHealthClient(conn)
	watcher, err := healthCheck.Watch(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}

	tick := time.Tick(time.Second)
	select {
	case <-tick:
		check, err := watcher.Recv()
		if err != nil {
			close(done)
			t.Fatal(err)
			return
		}
		if check.Status == healthgrpc.HealthCheckResponse_SERVING {
			done <- lis.Addr().String()
			return
		}
	case <-ctx.Done():
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
}

func testDiscoverSchemaTransform(t *testing.T, addr string, done func()) {
	ctx := context.Background()
	defer done()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := jobmanagement_v1.NewExpansionServiceClient(conn)
	got, err := client.DiscoverSchemaTransform(ctx, &jobmanagement_v1.DiscoverSchemaTransformRequest{})
	if err != nil {
		t.Fatal(err)
	}
	opts := []cmp.Option{
		cmpopts.IgnoreFields(pipeline_v1.Schema{}, "Id"),
		cmpopts.IgnoreUnexported(
			jobmanagement_v1.DiscoverSchemaTransformResponse{},
			jobmanagement_v1.SchemaTransformConfig{},
			pipeline_v1.Schema{},
		),
		cmp.Comparer(func(a, b *pipeline_v1.Field) bool {
			if a.Name != b.Name {
				return false
			}
			return a.Type.String() == a.Type.String()
		}),
	}

	want := &jobmanagement_v1.DiscoverSchemaTransformResponse{
		SchemaTransformConfigs: map[string]*jobmanagement_v1.SchemaTransformConfig{
			"hello": {
				ConfigSchema: &pipeline_v1.Schema{
					Fields: []*pipeline_v1.Field{
						{
							Name: "name",
							Type: &pipeline_v1.FieldType{
								TypeInfo: &pipeline_v1.FieldType_AtomicType{
									AtomicType: pipeline_v1.AtomicType_STRING,
								},
							},
						},
					},
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("DiscoverSchemaTransform() mismatch (-want, +got)\n%s", diff)
	}
}

func testExpand(t *testing.T, addr string, done func()) {
	ctx := context.Background()
	defer done()
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := jobmanagement_v1.NewExpansionServiceClient(conn)
	got, err := client.Expand(ctx, &jobmanagement_v1.ExpansionRequest{})
	if err != nil {
		t.Fatal(err)
	}

	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(
			jobmanagement_v1.ExpansionResponse{},
			pipeline_v1.Components{},
			pipeline_v1.PTransform{},
			pipeline_v1.PCollection{},
			pipeline_v1.Coder{},
			pipeline_v1.FunctionSpec{},
			pipeline_v1.WindowingStrategy{},
			pipeline_v1.Trigger{},
			pipeline_v1.Trigger_Default{},
		),
	}

	wantPayloadStr := "CsoBChliZWFtOmdvOnRyYW5zZm9ybTpkb2ZuOnYxGqwBQ2hsaVpXRnRPbWR2T25SeVlXNXpabTl5YlRwa2IyWnVPbll4RW1JS1J4SkJDQmdTUFFnYVNqbG5hWFJvZFdJdVkyOXRMMkZ3WVdOb1pTOWlaV0Z0TDNOa2EzTXZkakl2WjI4dmNHdG5MMkpsWVcwdlpYaHdZVzV6YVc5dUxtUnZSbTRhQW50OUVnZ0lBUklFQ2dJSURCb0dDZ1FLQWdnTUlnVlFZWEpFYnc9PQ=="
	wantPayload, err := base64.StdEncoding.DecodeString(wantPayloadStr)
	if err != nil {
		t.Fatal(err)
	}

	transform := &pipeline_v1.PTransform{
		UniqueName: "expansion.doFn",
		Spec: &pipeline_v1.FunctionSpec{
			Urn:     "beam:transform:pardo:v1",
			Payload: wantPayload,
		},
		Inputs:        map[string]string{"i0": "n2"},
		Outputs:       map[string]string{"i0": "n3"},
		EnvironmentId: "go",
	}

	want := &jobmanagement_v1.ExpansionResponse{
		Transform: transform,
		Components: &pipeline_v1.Components{
			Transforms: map[string]*pipeline_v1.PTransform{
				"e3": transform,
			},
			Pcollections: map[string]*pipeline_v1.PCollection{
				"n2": {
					UniqueName:          "n2",
					CoderId:             "c0",
					IsBounded:           pipeline_v1.IsBounded_BOUNDED,
					WindowingStrategyId: "w0",
				},
				"n3": {
					UniqueName:          "n3",
					CoderId:             "c0",
					IsBounded:           pipeline_v1.IsBounded_BOUNDED,
					WindowingStrategyId: "w0",
				},
			},
			Coders: map[string]*pipeline_v1.Coder{
				"c0": {
					Spec: &pipeline_v1.FunctionSpec{
						Urn: "beam:coder:string_utf8:v1",
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
					EnvironmentId:    "go",
				},
			},
		},
	}

	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("DiscoverSchemaTransform() mismatch (-want, +got)\n%s", diff)
	}
}

var _ Expander = &doFnX{}

type doFnX struct{}

func (d *doFnX) DoFn() reflect.Type {
	return reflect.TypeOf(doFn{})
}

func (d *doFnX) Config() reflect.Type {
	return reflect.TypeOf(struct {
		Name string `beam:"name"`
	}{})
}

func (d *doFnX) Uri() string {
	return "hello"
}

func (d *doFnX) Expand(s beam.Scope) {
	beam.ParDo(s, &doFn{}, beam.Create(s, ""))
}

type doFn struct{}

func (fn *doFn) ProcessElement(in string, emit func(string)) {

}
