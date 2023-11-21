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

package rrio

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rrio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	echo "github.com/apache/beam/sdks/v2/go/test/integration/io/rrio/echo/v1"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:generate cp -R ../../../../../../.test-infra/mock-apis/src/main/go/internal/proto/echo echo

const (
	grpcEndpointFlag             = "grpcEndpointAddress"
	httpEndpointFlag             = "httpEndpointAddress"
	echoPath                     = "v1/echo"
	echoShouldNeverExceedQuotaId = "echo-should-never-exceed-quota"
	echoShouldExceedQuotaId      = "echo-should-exceed-quota"
	timeout                      = time.Second * 3
	moreInfoUrl                  = "https://github.com/apache/beam/tree/master/.test-infra/mock-apis#integration"
)

var (
	moreInfo    = fmt.Sprintf("See %s for more information on how to get the relevant value for your test.", moreInfoUrl)
	grpcAddress = flag.String(grpcEndpointFlag, "", "The endpoint to target gRPC calls to the Echo service. "+moreInfo)
	httpAddress = flag.String(httpEndpointFlag, "", "The endpoint to target HTTP calls to the Echo service. "+moreInfo)
)

func init() {
	beam.RegisterType(reflect.TypeOf((*echoGrpcClient)(nil)).Elem())
	beam.RegisterDoFn(encodeFn)
	beam.RegisterDoFn(errMessageFn)
}

func TestExecute(t *testing.T) {
	grpcClient := &echoGrpcClient{
		Address: *grpcAddress,
	}
	httpClient := &echoHttpClient{
		Host: *httpAddress,
	}
	payload := []byte("payload")
	type args struct {
		caller        rrio.Caller
		setupTeardown rrio.SetupTeardown
	}
	for _, tt := range []struct {
		args            args
		quotaId         string
		numCalls        int
		drainQuotaFirst bool
		wantErrs        []any
	}{
		{
			args: args{
				caller:        grpcClient,
				setupTeardown: grpcClient,
			},
			quotaId:  echoShouldNeverExceedQuotaId,
			numCalls: 1,
		},
		{
			args: args{
				caller: httpClient,
			},
			quotaId:  echoShouldNeverExceedQuotaId,
			numCalls: 1,
		},
		{
			args: args{
				caller:        grpcClient,
				setupTeardown: grpcClient,
			},
			quotaId:         echoShouldExceedQuotaId,
			drainQuotaFirst: true,
			wantErrs: []any{
				"error: Echo(id:\"echo-should-exceed-quota\"  payload:\"payload\") err rpc error: code = ResourceExhausted desc = error: resource exhausted for: echo-should-exceed-quota",
			},
			numCalls: 1,
		},
	} {
		t.Run(tt.quotaId, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			if tt.drainQuotaFirst {
				drainQuota(ctx, t, grpcClient, tt.quotaId, 3)
			}

			var reqs []any
			var want []any

			var opts []rrio.Option
			if tt.args.setupTeardown != nil {
				opts = append(opts, rrio.WithSetupTeardown(tt.args.setupTeardown))
			}

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
				if len(tt.wantErrs) == 0 {
					want = append(want, &echo.EchoResponse{
						Id:      tt.quotaId,
						Payload: payload,
					})
				}
			}
			p, s := beam.NewPipelineWithRoot()
			s = s.WithContext(ctx, "rrio.TestExecute")
			input := beam.Create(s, reqs...)
			output, failures := rrio.Execute(s, grpcClient, input, opts...)
			got := beam.ParDo(s, encodeFn, output)

			errs := beam.ParDo(s, errMessageFn, failures)

			passert.Equals(s, errs, tt.wantErrs...)
			passert.Equals(s, got, want...)

			ptest.RunAndValidate(t, p)
		})
	}
}

// This method is needed in situations where we want to test quota errors.
// Due to the architecture constraints, we need a quota of at least 1.
// Therefore, it needs to be drained before testing expected exceeded quota
// errors.
func drainQuota(ctx context.Context, t *testing.T, client *echoGrpcClient, quotaId string, nCalls int) {
	t.Helper()
	if err := client.Setup(ctx); err != nil {
		t.Fatalf("error: echoGrpcClient.Setup() %v", err)
	}
	req := &echo.EchoRequest{
		Id:      quotaId,
		Payload: []byte(""),
	}
	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("error: proto.Marshal(echo.EchoRequest) %v", err)
	}
	for i := 0; i < nCalls; i++ {
		// we don't care about either the response or the error in this helper
		// method. The responses and errors are tested later.
		_, _ = client.Call(ctx, &rrio.Request{
			Payload: b,
		})
	}
}

// Verify interface implementation
var _ rrio.Caller = &echoGrpcClient{}
var _ rrio.SetupTeardown = &echoGrpcClient{}

type echoGrpcClient struct {
	Address  string
	conn     *grpc.ClientConn
	internal echo.EchoServiceClient
}

func (client *echoGrpcClient) Setup(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, client.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("error dialing gRPC endpoint: %s, err %w", client.Address, err)
	}
	client.conn = conn
	client.internal = echo.NewEchoServiceClient(conn)

	return nil
}

func (client *echoGrpcClient) Teardown(_ context.Context) error {
	return client.conn.Close()
}

func (client *echoGrpcClient) Call(ctx context.Context, request *rrio.Request) (*rrio.Response, error) {
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

// Verify interface implementation
var _ rrio.Caller = &echoHttpClient{}

type echoHttpClient struct {
	Host string
}

func (client *echoHttpClient) Call(ctx context.Context, request *rrio.Request) (*rrio.Response, error) {
	var req *echo.EchoRequest
	var response *echo.EchoResponse
	var body bytes.Buffer
	if err := proto.Unmarshal(request.Payload, req); err != nil {
		return nil, fmt.Errorf("proto.Unmarshall(rrio.Request.Payload) err %w", err)
	}
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return nil, fmt.Errorf("json.Encode(echo.EchoRequest) err %w", err)
	}
	rawUrl := path.Join(client.Host, echoPath)
	resp, err := http.DefaultClient.Post(rawUrl, "application/json", &body)
	if err != nil {
		return nil, fmt.Errorf("http Post(%s, %+v) err %w", rawUrl, req, err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("json.Decode(echo.EchoResponse) err %w", err)
	}
	b, err := proto.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("proto.Marshal(echo.EchoResponse) err %w", err)
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

func errMessageFn(err *rrio.ApiIOError) string {
	return err.Message
}

func TestMain(m *testing.M) {
	beam.Init()
	if !flag.Parsed() {
		flag.Parse()
	}
	var missing []string
	for _, f := range []string{
		grpcEndpointFlag,
		httpEndpointFlag,
	} {
		if flag.Lookup(f).Value.String() == "" {
			missing = append(missing, "--"+f)
		}
	}

	if len(missing) > 0 {
		panic(fmt.Sprintf("missing required flags: %s", strings.Join(missing, " ")))
	}

	os.Exit(m.Run())
}
