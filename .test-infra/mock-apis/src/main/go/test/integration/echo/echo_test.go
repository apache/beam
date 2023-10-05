package echo

import (
	"context"
	"testing"

	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/cache"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/logging"
	echov1 "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/proto/echo/v1"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/test_framework"
	"github.com/google/go-cmp/cmp"
)

var (
	logger = logging.New("github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration/echo")
)

func TestEcho(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	refresher, err := cache.NewRefresher(ctx, test_framework.Cache)
	if err != nil {
		t.Fatalf("error instantiating %T, err %v", refresher, err)
	}
	errChan := make(chan error)
	ready := make(chan struct{})

	go test_framework.Serve(ctx, errChan, ready)

	for {
		select {
		case err := <-errChan:
			t.Fatal(err)
		case <-ready:
			testEcho(t)
			return
		case <-ctx.Done():
			return
		}
	}
}

func testEcho(t *testing.T) {
	for _, tt := range []struct {
		name    string
		req     *echov1.EchoRequest
		want    *echov1.EchoResponse
		wantErr error
	}{
		{
			name: "quota available; correct id",
			req: &echov1.EchoRequest{
				Id:      test_framework.QuotaId,
				Payload: []byte(test_framework.QuotaId),
			},
			want: &echov1.EchoResponse{
				Id:      test_framework.QuotaId,
				Payload: []byte(test_framework.QuotaId),
			},
			wantErr: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := test_framework.EchoClient.Echo(context.Background(), tt.req)
			if err != nil && tt.wantErr == nil {
				t.Errorf("Echo(%+v) err %v", tt.req, err)
				return
			}
			if diff := cmp.Diff(got, tt.want); diff != "" {
				t.Errorf("Echo(%+v) = %+v, want %+v, diff:\n%s", tt.req, got, tt.want, diff)
			}
		})
	}

}

func TestMain(m *testing.M) {
	ctx := context.Background()
	if err := test_framework.Run(ctx, m); err != nil {
		logger.Error(ctx, err, test_framework.LoggerFields...)
	}
}
