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

// boot is the boot code for the Java SDK harness container. It is responsible
// for retrieving staged files and invoking the JVM correctly.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/provision"
	"github.com/apache/beam/sdks/go/pkg/beam/util/execx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/syscallx"
	"github.com/golang/protobuf/proto"
)

var (
	// Contract: https://s.apache.org/beam-fn-api-container-contract.

	id                = flag.String("id", "", "Local identifier (required).")
	loggingEndpoint   = flag.String("logging_endpoint", "", "Logging endpoint (required).")
	artifactEndpoint  = flag.String("artifact_endpoint", "", "Artifact endpoint (required).")
	provisionEndpoint = flag.String("provision_endpoint", "", "Provision endpoint (required).")
	controlEndpoint   = flag.String("control_endpoint", "", "Control endpoint (required).")
	semiPersistDir    = flag.String("semi_persist_dir", "/tmp", "Local semi-persistent directory (optional).")
)

func main() {
	flag.Parse()
	if *id == "" {
		log.Fatal("No id provided.")
	}
	if *loggingEndpoint == "" {
		log.Fatal("No logging endpoint provided.")
	}
	if *artifactEndpoint == "" {
		log.Fatal("No artifact endpoint provided.")
	}
	if *provisionEndpoint == "" {
		log.Fatal("No provision endpoint provided.")
	}
	if *controlEndpoint == "" {
		log.Fatal("No control endpoint provided.")
	}

	log.Printf("Initializing java harness: %v", strings.Join(os.Args, " "))

	ctx := grpcx.WriteWorkerID(context.Background(), *id)

	// (1) Obtain the pipeline options

	info, err := provision.Info(ctx, *provisionEndpoint)
	if err != nil {
		log.Fatalf("Failed to obtain provisioning information: %v", err)
	}
	options, err := provision.ProtoToJSON(info.GetPipelineOptions())
	if err != nil {
		log.Fatalf("Failed to convert pipeline options: %v", err)
	}

	// (2) Retrieve the staged user jars. We ignore any disk limit,
	// because the staged jars are mandatory.

	dir := filepath.Join(*semiPersistDir, "staged")

	artifacts, err := artifact.Materialize(ctx, *artifactEndpoint, info.GetRetrievalToken(), dir)
	if err != nil {
		log.Fatalf("Failed to retrieve staged files: %v", err)
	}

	// (3) Invoke the Java harness, preserving artifact ordering in classpath.

	os.Setenv("HARNESS_ID", *id)
	os.Setenv("PIPELINE_OPTIONS", options)
	os.Setenv("LOGGING_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pb.ApiServiceDescriptor{Url: *loggingEndpoint}))
	os.Setenv("CONTROL_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(&pb.ApiServiceDescriptor{Url: *controlEndpoint}))

	if info.GetStatusEndpoint() != nil {
		os.Setenv("STATUS_API_SERVICE_DESCRIPTOR", proto.MarshalTextString(info.GetStatusEndpoint()))
	}

	const jarsDir = "/opt/apache/beam/jars"
	cp := []string{
		filepath.Join(jarsDir, "slf4j-api.jar"),
		filepath.Join(jarsDir, "slf4j-jdk14.jar"),
		filepath.Join(jarsDir, "beam-sdks-java-harness.jar"),
		filepath.Join(jarsDir, "beam-sdks-java-io-kafka.jar"),
		filepath.Join(jarsDir, "kafka-clients.jar"),
	}

	var hasWorkerExperiment = strings.Contains(options, "use_staged_dataflow_worker_jar")
	for _, md := range artifacts {
		if hasWorkerExperiment {
			if strings.HasPrefix(md.Name, "beam-runners-google-cloud-dataflow-java-fn-api-worker") {
				continue
			}
			if md.Name == "dataflow-worker.jar" {
				continue
			}
		}
		cp = append(cp, filepath.Join(dir, filepath.FromSlash(md.Name)))
	}

	args := []string{
		"-Xmx" + strconv.FormatUint(heapSizeLimit(info), 10),
		"-XX:-OmitStackTraceInFastThrow",
		"-cp", strings.Join(cp, ":"),
		"org.apache.beam.fn.harness.FnHarness",
	}

	log.Printf("Executing: java %v", strings.Join(args, " "))

	log.Fatalf("Java exited: %v", execx.Execute("java", args...))
}

// heapSizeLimit returns 80% of the runner limit, if provided. If not provided,
// it returns 70% of the physical memory on the machine. If it cannot determine
// that value, it returns 1GB. This is an imperfect heuristic. It aims to
// ensure there is memory for non-heap use and other overhead, while also not
// underutilizing the machine.
func heapSizeLimit(info *fnpb.ProvisionInfo) uint64 {
	if provided := info.GetResourceLimits().GetMemory().GetSize(); provided > 0 {
		return (provided * 80) / 100
	}
	if size, err := syscallx.PhysicalMemorySize(); err == nil {
		return (size * 70) / 100
	}
	return 1 << 30
}
