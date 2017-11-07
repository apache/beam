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

// gcemd is a metadata-configured provisioning server for GCE.
package main

import (
	"flag"
	"log"
	"net"

	"cloud.google.com/go/compute/metadata"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	endpoint = flag.String("endpoint", "", "Server endpoint to expose.")
)

func main() {
	flag.Parse()
	if *endpoint == "" {
		log.Fatal("No endpoint provided. Use --endpoint=localhost:12345")
	}
	if !metadata.OnGCE() {
		log.Fatal("Not running on GCE")
	}

	log.Printf("Starting provisioning server on %v", *endpoint)

	jobID, err := metadata.InstanceAttributeValue("job_id")
	if err != nil {
		log.Fatalf("Failed to find job ID: %v", err)
	}
	jobName, err := metadata.InstanceAttributeValue("job_name")
	if err != nil {
		log.Fatalf("Failed to find job name: %v", err)
	}
	opt, err := metadata.InstanceAttributeValue("sdk_pipeline_options")
	if err != nil {
		log.Fatalf("Failed to find SDK pipeline options: %v", err)
	}
	options, err := provision.JSONToProto(opt)
	if err != nil {
		log.Fatalf("Failed to parse SDK pipeline options: %v", err)
	}

	info := &pb.ProvisionInfo{
		JobId:           jobID,
		JobName:         jobName,
		PipelineOptions: options,
	}

	gs := grpc.NewServer()
	pb.RegisterProvisionServiceServer(gs, &server{info: info})

	listener, err := net.Listen("tcp", *endpoint)
	if err != nil {
		log.Fatalf("Failed to listen to %v: %v", *endpoint, err)
	}
	log.Fatalf("Server failed: %v", gs.Serve(listener))
}

type server struct {
	info *pb.ProvisionInfo
}

func (s *server) GetProvisionInfo(ctx context.Context, req *pb.GetProvisionInfoRequest) (*pb.GetProvisionInfoResponse, error) {
	return &pb.GetProvisionInfoResponse{Info: s.info}, nil
}
