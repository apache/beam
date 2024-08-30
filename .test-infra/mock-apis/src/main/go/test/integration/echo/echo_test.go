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

// Tests for the src/main/go/cmd/service/echo service.
package echo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"testing"
	"time"

	echov1 "github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/proto/echo/v1"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/internal/service/echo"
	"github.com/apache/beam/test-infra/mock-apis/src/main/go/test/integration"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// QuotaIds below correspond to:
	// kubectl get deploy --selector=app.kubernetes.io/tag=refresher -o custom-columns='QUOTA_ID:.metadata.labels.quota-id'
	// See https://github.com/apache/beam/tree/master/.test-infra/mock-apis#writing-integration-tests
	shouldExceedQuotaId      = "echo-should-exceed-quota"
	shouldNeverExceedQuotaId = "echo-should-never-exceed-quota"
	shouldNotExistId         = "should-not-exist"
	refresh10Per1s           = "echo-10-per-1s-quota"
	defaultNumCalls          = 3
)

var (
	grpcOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	timeout = time.Second * 3
)

func TestEcho(t *testing.T) {
	payload := []byte("payload")

	for _, tt := range []struct {
		tag      string
		quotaId  string
		client   echov1.EchoServiceClient
		want     *echov1.EchoResponse
		numCalls int
		wantErr  error
	}{
		{
			tag:     "http",
			quotaId: shouldExceedQuotaId,
			client:  withHttp(t),
			wantErr: errors.New("429 Too Many Requests"),
		},
		{
			tag:     "grpc",
			quotaId: shouldExceedQuotaId,
			client:  withGrpc(t),
			wantErr: status.Error(codes.ResourceExhausted, "error: resource exhausted for: echo-should-exceed-quota"),
		},
		{
			tag:     "http",
			quotaId: shouldNotExistId,
			client:  withHttp(t),
			wantErr: errors.New("404 Not Found"),
		},
		{
			tag:     "grpc",
			quotaId: shouldNotExistId,
			client:  withGrpc(t),
			wantErr: status.Error(codes.NotFound, "error: source not found: should-not-exist, err resource does not exist"),
		},
		{
			tag:     "http",
			quotaId: shouldNeverExceedQuotaId,
			client:  withHttp(t),
			want: &echov1.EchoResponse{
				Id:      shouldNeverExceedQuotaId,
				Payload: payload,
			},
		},
		{
			tag:     "grpc",
			quotaId: shouldNeverExceedQuotaId,
			client:  withGrpc(t),
			want: &echov1.EchoResponse{
				Id:      shouldNeverExceedQuotaId,
				Payload: payload,
			},
		},
		{
			numCalls: 20,
			tag:      "grpc",
			quotaId:  refresh10Per1s,
			client:   withGrpc(t),
			wantErr:  status.Error(codes.ResourceExhausted, "error: resource exhausted for: echo-10-per-1s-quota"),
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", tt.quotaId, tt.tag), func(t *testing.T) {
			ctx, cancel := withTimeout()
			defer cancel()

			if tt.numCalls == 0 {
				tt.numCalls = defaultNumCalls
			}

			wg := sync.WaitGroup{}
			wg.Add(tt.numCalls)

			req := &echov1.EchoRequest{
				Id:      tt.quotaId,
				Payload: payload,
			}

			var resps []*echov1.EchoResponse
			var errs []error

			for i := 0; i < tt.numCalls; i++ {
				go func() {
					resp, err := tt.client.Echo(ctx, req)
					if err != nil {
						errs = append(errs, err)
					}
					if resp != nil {
						resps = append(resps, resp)
					}
					wg.Done()
				}()
			}

			wg.Wait()

			if tt.wantErr != nil && len(errs) == 0 {
				t.Errorf("Echo(%+v) err = nil, wantErr = %v", req, tt.wantErr)
				return
			}

			for _, err := range errs {
				if diff := cmp.Diff(tt.wantErr.Error(), err.Error()); diff != "" {
					t.Errorf("Echo(%+v) err mismatch (-want +got)\n%s", req, diff)
				}
			}

			if tt.want != nil {
				for _, resp := range resps {
					if diff := cmp.Diff(tt.want, resp, cmpopts.IgnoreUnexported(echov1.EchoResponse{})); diff != "" {
						t.Errorf("Echo(%+v) mismatch (-want +got)\n%s", req, diff)
					}
				}
			}

		})
	}
}

func TestMain(m *testing.M) {
	integration.Run(m)
}

func withGrpc(t *testing.T) echov1.EchoServiceClient {
	t.Helper()
	ctx, cancel := withTimeout()
	defer cancel()

	if *integration.GRPCServiceEndpoint == "" {
		t.Fatalf("missing flag: -%s", integration.GrpcServiceEndpointFlag)
	}

	conn, err := grpc.DialContext(ctx, *integration.GRPCServiceEndpoint, grpcOpts...)
	if err != nil {
		t.Fatalf("DialContext(%s) err %v", *integration.GRPCServiceEndpoint, err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	})

	return echov1.NewEchoServiceClient(conn)
}

type httpCaller struct {
	rawUrl string
}

func (h *httpCaller) Echo(ctx context.Context, in *echov1.EchoRequest, _ ...grpc.CallOption) (*echov1.EchoResponse, error) {
	ctx, cancel := withTimeout()
	defer cancel()
	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(in); err != nil {
		return nil, err
	}

	resp, err := http.Post(h.rawUrl, "application/json", &buf)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode > 299 {
		return nil, errors.New(resp.Status)
	}

	var result *echov1.EchoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func withHttp(t *testing.T) echov1.EchoServiceClient {
	if *integration.HTTPServiceEndpoint == "" {
		t.Fatalf("missing flag: -%s", integration.HttpServiceEndpointFlag)
	}
	p := regexp.MustCompile(`^http://`)
	rawUrl := fmt.Sprint(*integration.HTTPServiceEndpoint, echo.PathAlias)
	if !p.MatchString(rawUrl) {
		t.Fatalf("missing 'http(s)' scheme from %s", *integration.HTTPServiceEndpoint)
	}
	return &httpCaller{
		rawUrl: rawUrl,
	}
}

func withTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
