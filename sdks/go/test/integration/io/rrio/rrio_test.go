package rrio

import (
	"context"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rrio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	echo "github.com/apache/beam/sdks/v2/go/test/integration/io/rrio/echo/v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"reflect"
	"testing"
	"time"
)

//go:generate cp -R ../../../../../../.test-infra/mock-apis/src/main/go/internal/proto/echo echo

const (
	echoShouldNeverExceedQuotaId = "echo-should-never-exceed-quota"
	timeout                      = time.Hour * 3
)

func init() {
	beam.RegisterType(reflect.TypeOf((*echoClient)(nil)).Elem())
	beam.RegisterDoFn(encodeFn)
}

func TestExecute(t *testing.T) {
	client := &echoClient{
		Address: "localhost:50051",
	}
	payload := []byte("payload")
	for _, tt := range []struct {
		quotaId      string
		numCalls     int
		wantFailures bool
	}{
		{
			quotaId:  echoShouldNeverExceedQuotaId,
			numCalls: 1,
		},
	} {
		t.Run(tt.quotaId, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			var reqs []any
			var want []any
			for i := 0; i < tt.numCalls; i++ {
				b, err := proto.Marshal(&echo.EchoRequest{
					Id:      tt.quotaId,
					Payload: payload,
				})
				if err != nil {
					t.Fatalf("error proto.Marshal(echo.EchoRequest) %v", err)
				}
				reqs = append(reqs, &rrio.Request{
					Payload: b,
				})
				if !tt.wantFailures {
					want = append(want, &echo.EchoResponse{
						Id:      tt.quotaId,
						Payload: payload,
					})
				}
			}
			p, s := beam.NewPipelineWithRoot()
			s = s.WithContext(ctx, "rrio.TestExecute")
			input := beam.Create(s, reqs...)
			output, failures := rrio.Execute(s, client, input, rrio.WithSetupTeardown(client))
			got := beam.ParDo(s, encodeFn, output)

			wantFailureCount := 0
			if tt.wantFailures {
				wantFailureCount = tt.numCalls
			}
			passert.Count(s, failures, "failures", wantFailureCount)
			passert.Equals(s, got, want...)

			ptest.RunAndValidate(t, p)
		})
	}
}

var _ rrio.Caller = &echoClient{}
var _ rrio.SetupTeardown = &echoClient{}

type echoClient struct {
	Address  string
	conn     *grpc.ClientConn
	internal echo.EchoServiceClient
}

func (client *echoClient) Setup(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, client.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("error dialing gRPC endpoint: %s, err %w", client.Address, err)
	}
	client.conn = conn
	client.internal = echo.NewEchoServiceClient(conn)

	return nil
}

func (client *echoClient) Teardown(_ context.Context) error {
	return client.conn.Close()
}

func (client *echoClient) Call(ctx context.Context, request *rrio.Request) (*rrio.Response, error) {
	var req echo.EchoRequest
	if err := proto.Unmarshal(request.Payload, &req); err != nil {
		return nil, fmt.Errorf("proto.Unmarshall(rrio.Request.Payload) err %w", err)
	}
	resp, err := client.internal.Echo(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("error: Echo(%+v) err %w", &req, err)
	}
	b, err := proto.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("proto.Marshall(%+v) err %w", resp, err)
	}
	return &rrio.Response{
		Payload: b,
	}, nil
}

func encodeFn(resp *rrio.Response) (*echo.EchoResponse, error) {
	var v echo.EchoResponse
	if err := proto.Unmarshal(resp.Payload, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func TestMain(m *testing.M) {
	ptest.Main(m)
}
