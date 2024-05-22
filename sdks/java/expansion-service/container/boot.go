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

// Boot code for the Java SDK expansion service.
// Contract:
//   https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_expansion_api.proto
//   https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_artifact_api.proto
package main

import (
	// 	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
)

// Args:
//   - Expansion service port
//   - Dependencies (for loading SchemaTransforms)
//   - Config file path. Config file contains:
//     - Allow-list
//     - Per-transform dependencies config.
var (
	id               = flag.String("id", "", "Local identifier (required)")
	port             = flag.Int("port", 0, "Port for the expansion service (required)")
	dependencies_dir = flag.String("dependencies_dir", "", "A directory containing the set of jar files to load transforms from (required)")
	config_file      = flag.String("config_file", "", "Expansion service config YAML file. (required)")
	use_alts      = flag.Bool("use_alts", false, "Starts an Expansion Service with support for gRPC ALTS authentication")
)

const entrypoint = "org.apache.beam.sdk.expansion.service.ExpansionService"

func main() {
	flag.Parse()

	if *id == "" {
		log.Fatalf("The flag 'id' was not specified")
	}
	if *port == 0 {
		log.Fatalf("The flag 'port' was not specified")
	}
	if *dependencies_dir == "" {
		log.Fatalf("The flag 'dependencies_dir' was not specified")
	}
	if *config_file == "" {
		log.Fatalf("The flag 'config_file' was not specified")
	}

	log.Printf("Starting the Java expansion service container %v.", *id)

	// Determine all jar files from the dipendencies_dir to be used for the CLASSPATH.
	files, _ := ioutil.ReadDir(*dependencies_dir)
	cp := []string{}
	path, _ := os.Getwd()
	for _, file := range files {
		cp = append(cp, filepath.Join(path, *dependencies_dir, file.Name()))
	}

	args := []string{
		// Seting max RAM percentage to a high value since we are running a single JVM within the container.
		"-XX:MaxRAMPercentage=80.0",
		// Keep following JVM options in sync with other Java containers released with Beam.
		"-XX:+UseParallelGC",
		"-XX:+AlwaysActAsServerClassMachine",
		"-XX:-OmitStackTraceInFastThrow",
		"-cp", strings.Join(cp, ":"),
	}

	args = append(args, entrypoint)
	args = append(args, strconv.Itoa(*port))

	if *config_file != "" {
		args = append(args, fmt.Sprintf("--expansionServiceConfigFile=%s", *config_file))
	}
	if *use_alts {
		args = append(args, "--useAltsServer=true")
	}

	log.Printf("Executing: java %v", strings.Join(args, " "))
	log.Fatalf("Java exited: %v", execx.Execute("java", args...))
}
