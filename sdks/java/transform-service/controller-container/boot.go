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

// Boot code for the transform service controller.
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
//   - Transform service port
//   - Config file path. Config file contains:
//     - A list of expansion services
var (
	port        = flag.Int("port", 0, "Port for the expansion service (required)")
	config_file = flag.String("config_file", "", "Transform service config YAML file. (required)")
)

const entrypoint = "org.apache.beam.sdk.transformservice.Controller"

func main() {
	flag.Parse()

	if *port == 0 {
		log.Fatalf("The flag 'port' was not specified")
	}

	if *config_file == "" {
		log.Fatalf("The flag 'config_file' was not specified")
	}

	log.Printf("Starting the transform service controller container")

	// Determine all jar files from the 'jars' to be used for the CLASSPATH.
	files, _ := ioutil.ReadDir("jars")
	cp := []string{}
	path, _ := os.Getwd()
	for _, file := range files {
		cp = append(cp, filepath.Join(path, "jars", file.Name()))
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

	if *port != 0 {
		args = append(args, fmt.Sprintf("--port=%s", strconv.Itoa(*port)))
	}
	if *config_file != "" {
		args = append(args, fmt.Sprintf("--transformServiceConfigFile=%s", *config_file))
	}

	log.Printf("Executing: java %v", strings.Join(args, " "))
	log.Fatalf("Java exited: %v", execx.Execute("java", args...))
}
