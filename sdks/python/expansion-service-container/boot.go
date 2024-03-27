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
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx/expansionx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/execx"
)

var (
	id                = flag.String("id", "", "Local identifier (required)")
	port              = flag.Int("port", 0, "Port for the expansion service (required)")
	requirements_file = flag.String("requirements_file", "", "A requirement file with extra packages to be made available to the transforms being expanded. Path should be relative to the 'dependencies_dir'")
	dependencies_dir  = flag.String("dependencies_dir", "", "A directory that stores locally available extra packages.")
)

const (
	expansionServiceEntrypoint = "apache_beam.runners.portability.expansion_service_main"
	venvDirectory              = "beam_venv" // This should match the venv directory name used in the Dockerfile.
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

func getLines(fileNameToRead string) ([]string, error) {
	fileToRead, err := os.Open(fileNameToRead)
	if err != nil {
		return nil, err
	}
	defer fileToRead.Close()

	sc := bufio.NewScanner(fileToRead)
	lines := make([]string, 0)

	// Read through 'tokens' until an EOF is encountered.
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func installExtraPackages(requirementsFile string) error {
	extraPackages, err := getLines(requirementsFile)
	if err != nil {
		return err
	}

	for _, extraPackage := range extraPackages {
		log.Printf("Installing extra package %v", extraPackage)
		// We expect 'pip' command in virtual env to be already available at the top of the PATH.
		args := []string{"install", extraPackage}
		if err := execx.Execute("pip", args...); err != nil {
			return fmt.Errorf("Could not install the package %s: %s", extraPackage, err)
		}
	}
	return nil
}

func getUpdatedRequirementsFile(oldDependenciesRequirementsFile string, dependenciesDir string) (string, error) {
	oldExtraPackages, err := getLines(oldDependenciesRequirementsFile)
	if err != nil {
		return "", err
	}
	var updatedExtraPackages = make([]string, 0)
	for _, extraPackage := range oldExtraPackages {
		// TODO update
		potentialLocalFilePath := filepath.Join(dependenciesDir, extraPackage)
		_, err := os.Stat(potentialLocalFilePath)
		if err == nil {
			// Package exists locally so using that.
			extraPackage = potentialLocalFilePath
			log.Printf("Using locally available extra package %v", extraPackage)
		}
		updatedExtraPackages = append(updatedExtraPackages, extraPackage)
	}

	updatedRequirementsFile, err := ioutil.TempFile("/opt/apache/beam", "requirements*.txt")
	if err != nil {
		return "", err
	}

	updatedRequirementsFileName := updatedRequirementsFile.Name()

	datawriter := bufio.NewWriter(updatedRequirementsFile)
	for _, extraPackage := range updatedExtraPackages {
		_, _ = datawriter.WriteString(extraPackage + "\n")
	}
	datawriter.Flush()
	updatedRequirementsFile.Close()

	return updatedRequirementsFileName, nil
}

func launchExpansionServiceProcess() error {
	pythonVersion, err := expansionx.GetPythonVersion()
	if err != nil {
		return err
	}
	log.Printf("Starting Python expansion service ...")

	dir := filepath.Join("/opt/apache/beam", venvDirectory)
	os.Setenv("VIRTUAL_ENV", dir)
	os.Setenv("PATH", strings.Join([]string{filepath.Join(dir, "bin"), os.Getenv("PATH")}, ":"))

	args := []string{"-m", expansionServiceEntrypoint, "-p", strconv.Itoa(*port), "--fully_qualified_name_glob", "*"}

	// Requirements file with dependencies to install.
	// Note that we have to look for the requirements file in the dependencies
	// volume here not the requirements file at the top level. Latter provides
	// Beam dependencies.
	dependencies_requirements_file := filepath.Join(*dependencies_dir, *requirements_file)
	dependencies_requirements_file_exists := false
	if _, err := os.Stat(dependencies_requirements_file); err == nil {
		dependencies_requirements_file_exists = true
	}

	// We only try to install dependencies, if the requirements file exists.
	if dependencies_requirements_file_exists {
		log.Printf("Received the requirements file %s with extra packages.", dependencies_requirements_file)
		updatedRequirementsFileName, err := getUpdatedRequirementsFile(dependencies_requirements_file, *dependencies_dir)
		if err != nil {
			return err
		}
		defer os.Remove(updatedRequirementsFileName)
		log.Printf("Updated requirements file is %v", updatedRequirementsFileName)
		// Provide the requirements file to the expansion service so that packages get staged by runners.
		args = append(args, "--requirements_file", updatedRequirementsFileName)
		// Install packages locally so that they can be used by the expansion service during transform
		// expansion if needed.
		err = installExtraPackages(updatedRequirementsFileName)
		if err != nil {
			return err
		}
	} else {
		log.Printf("Requirements file %s was provided but not available.", dependencies_requirements_file)
	}

	if err := execx.Execute(pythonVersion, args...); err != nil {
		return fmt.Errorf("could not start the expansion service: %s", err)
	}

	return nil
}
