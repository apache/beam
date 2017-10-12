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

// gcsproxy is an artifact server backed by GCS and can run in either retrieval
// (read) or staging (write) mode.
package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact/gcsproxy"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
)

const (
	retrieve = "retrieve"
	stage    = "stage"
)

var (
	mode     = flag.String("mode", retrieve, "Proxy mode: retrieve or stage.")
	endpoint = flag.String("endpoint", "", "Server endpoint to expose.")
	manifest = flag.String("manifest", "", "Location of proxy manifest.")
)

func main() {
	flag.Parse()
	if *manifest == "" {
		log.Fatal("No proxy manifest location provided. Use --manifest=gs://foo/bar")
	}
	if *endpoint == "" {
		log.Fatal("No endpoint provided. Use --endpoint=localhost:12345")
	}

	gs := grpc.NewServer()

	switch *mode {
	case retrieve:
		// Retrieval mode. We download the manifest -- but not the
		// artifacts -- eagerly.

		log.Printf("Starting retrieval proxy from %v on %v", *manifest, *endpoint)

		md, err := gcsproxy.ReadProxyManifest(context.Background(), *manifest)
		if err != nil {
			log.Fatalf("Failed to obtain proxy manifest %v: %v", *manifest, err)
		}
		proxy, err := gcsproxy.NewRetrievalServer(md)
		if err != nil {
			log.Fatalf("Failed to create artifact server: %v", err)
		}
		pb.RegisterArtifactRetrievalServiceServer(gs, proxy)

	case stage:
		// Staging proxy. We update the blobs next to the manifest
		// in a blobs "directory".

		log.Printf("Starting staging proxy to %v on %v", *manifest, *endpoint)

		proxy, err := gcsproxy.NewStagingServer(*manifest)
		if err != nil {
			log.Fatalf("Failed to create artifact server: %v", err)
		}
		pb.RegisterArtifactStagingServiceServer(gs, proxy)

	default:
		log.Fatalf("Invalid mode: '%v', want '%v' or '%v'", *mode, retrieve, stage)
	}

	listener, err := net.Listen("tcp", *endpoint)
	if err != nil {
		log.Fatalf("Failed to listen to %v: %v", *endpoint, err)
	}
	log.Fatalf("Server failed: %v", gs.Serve(listener))
}
