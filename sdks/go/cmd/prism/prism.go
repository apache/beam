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

// prism is a stand alone local Beam Runner. It produces a JobManagement service endpoint
// against which jobs can be submited, and a web UI to inspect running and completed jobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	jobPort            = flag.Int("job_port", 8073, "specify the job management service port")
	webPort            = flag.Int("web_port", 8074, "specify the web ui port")
	jobManagerEndpoint = flag.String("jm_override", "", "set to only stand up a web ui that refers to a seperate JobManagement endpoint")
	serveHTTP          = flag.Bool("serve_http", true, "enable or disable the web ui")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	cli, err := makeJobClient(ctx, prism.Options{Port: *jobPort}, *jobManagerEndpoint)
	if err != nil {
		log.Fatalf("error creating job server: %v", err)
	}
	if *serveHTTP {
		if err := prism.CreateWebServer(ctx, cli, prism.Options{Port: *webPort}); err != nil {
			log.Fatalf("error creating web server: %v", err)
		}
	} else {
		// Block main thread forever to keep main from exiting.
		<-(chan struct{})(nil) // receives on nil channels block.
	}
}

func makeJobClient(ctx context.Context, opts prism.Options, endpoint string) (jobpb.JobServiceClient, error) {
	if endpoint != "" {
		clientConn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return nil, fmt.Errorf("error connecting to job server at %v: %v", endpoint, err)
		}
		return jobpb.NewJobServiceClient(clientConn), nil
	}
	cli, err := prism.CreateJobServer(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("error creating local job server: %v", err)
	}
	return cli, nil
}
