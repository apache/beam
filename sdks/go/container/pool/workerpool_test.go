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

package pool

import (
	"context"
	"os/exec"
	"testing"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

func TestProcess(t *testing.T) {
	// Use the no-op true binary, if available, skip this test otherwise.
	dummyExec, err := exec.LookPath("true")
	if err != nil {
		t.Skip("Binary `true` doesn't exist, skipping tests.")
	}

	endpoint := &pipepb.ApiServiceDescriptor{
		Url: "localhost:0",
	}
	secureEndpoint := &pipepb.ApiServiceDescriptor{
		Url: "localhost:0",
		Authentication: &pipepb.AuthenticationSpec{
			Urn: "beam:authentication:oauth2_client_credentials_grant:v1",
		},
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)
	server, err := New(ctx, 0, dummyExec)
	if err != nil {
		t.Fatalf("Unable to create server: %v", err)
	}
	go server.ServeAndWait()

	startTests := []struct {
		req         *fnpb.StartWorkerRequest
		errExpected bool
	}{
		{
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "Worker1",
				ControlEndpoint: endpoint,
				LoggingEndpoint: endpoint,
			},
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "Worker2",
				ControlEndpoint: endpoint,
				LoggingEndpoint: endpoint,
			},
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "Worker1",
				ControlEndpoint: endpoint,
				LoggingEndpoint: endpoint,
			},
			errExpected: true, // Repeated start
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "missingControl",
				LoggingEndpoint: endpoint,
			},
			errExpected: true,
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "missingLogging",
				ControlEndpoint: endpoint,
			},
			errExpected: true,
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "secureLogging",
				LoggingEndpoint: secureEndpoint,
				ControlEndpoint: endpoint,
			},
			errExpected: true,
		}, {
			req: &fnpb.StartWorkerRequest{
				WorkerId:        "secureControl",
				LoggingEndpoint: endpoint,
				ControlEndpoint: secureEndpoint,
			},
			errExpected: true,
		},
	}
	for _, test := range startTests {
		resp, err := server.StartWorker(ctx, test.req)
		if test.errExpected {
			if err != nil || resp.Error == "" {
				t.Errorf("Expected error starting %v: err: %v, resp: %v", test.req.GetWorkerId(), err, resp)
			}
		} else {
			if err != nil || resp.Error != "" {
				t.Errorf("Unexpected error starting %v: err: %v, resp: %v", test.req.GetWorkerId(), err, resp)
			}
		}
	}
	stopTests := []struct {
		req         *fnpb.StopWorkerRequest
		errExpected bool
	}{
		{
			req: &fnpb.StopWorkerRequest{
				WorkerId: "Worker1",
			},
		}, {
			req: &fnpb.StopWorkerRequest{
				WorkerId: "Worker1",
			},
			errExpected: true,
		}, {
			req: &fnpb.StopWorkerRequest{
				WorkerId: "NonExistent",
			},
			errExpected: true,
		},
	}
	for _, test := range stopTests {
		resp, err := server.StopWorker(ctx, test.req)
		if test.errExpected {
			if err != nil || resp.Error == "" {
				t.Errorf("Expected error starting %v: err: %v, resp: %v", test.req.GetWorkerId(), err, resp)
			}
		} else {
			if err != nil || resp.Error != "" {
				t.Errorf("Unexpected error starting %v: err: %v, resp: %v", test.req.GetWorkerId(), err, resp)
			}
		}
	}
	if err := server.Stop(ctx); err != nil {
		t.Fatalf("error stopping server: err: %v", err)
	}
}
