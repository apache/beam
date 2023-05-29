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

// boot is the boot code for the Python SDK harness container. It is responsible
// for retrieving and install staged files and invoking python correctly.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
)

var (
	id   = flag.String("id", "", "Local identifier (required)")
	port = flag.Int("port", 0, "Port for the expansion service (required)")
)

const (
	expansionServiceEntrypoint = "apache_beam.runners.portability.expansion_service_main"
	venvDirectory              = "beam_venv" // This should match the venv directory name used in the Dockerfile.
	requirementsFile           = "requirements.txt"
	beamSDKArtifact            = "apache-beam-sdk.tar.gz"
	beamSDKOptions             = "[gcp,dataframe]"
)

func main() {
	flag.Parse()

	if *id == "" {
		log.Fatalf("The flag 'id' was not specified")
	}
	if *port == 0 {
		log.Fatalf("The flag 'port' was not specified")
	}

	if err := launchExpansionServiceProcess(); err != nil {
		log.Fatal(err)
	}
}

func launchExpansionServiceProcess() error {
	log.Printf("Starting Python expansion service ...")

	dir := filepath.Join("/opt/apache/beam", venvDirectory)
	os.Setenv("VIRTUAL_ENV", dir)
	os.Setenv("PATH", strings.Join([]string{filepath.Join(dir, "bin"), os.Getenv("PATH")}, ":"))

	args := []string{"-m", expansionServiceEntrypoint, "-p", strconv.Itoa(*port), "--fully_qualified_name_glob", "*"}
	if err := execx.Execute("python", args...); err != nil {
		return fmt.Errorf("Could not start the expansion service: %s", err)
	}

	return nil
}
