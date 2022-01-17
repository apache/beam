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

// Package expansionx contains utilities for starting expansion services for
// cross-language transforms. All code in this package is currently experimental.
package expansionx

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
)

type url string

type jarGetter struct {
	repository url
	groupID    string
	jarCache   string
}

var defaultJarGetter = newJarGetter()

const (
	apacheRepository = url("https://repo.maven.apache.org/maven2")
	beamGroupID      = "org/apache/beam"
	jarCache         = "~/.apache_beam/cache/jars"
)

func newJarGetter() *jarGetter {
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])
	return &jarGetter{repository: apacheRepository, groupID: beamGroupID, jarCache: cacheDir}
}

// GetBeamJar checks a temporary directory for the desired Beam JAR, downloads the
// appropriate JAR from Maven if not present, then returns the file path to the
// JAR.
func GetBeamJar(gradleTarget, version string) (string, error) {
	return defaultJarGetter.getJar(gradleTarget, version)
}

func (j *jarGetter) getJar(gradleTarget, version string) (string, error) {
	strippedTarget := dropEndOfGradleTarget(gradleTarget)
	fullURL, jarName := j.getURLForBeamJar(strippedTarget, version)

	err := os.MkdirAll(j.jarCache, 0700)
	if err != nil {
		return "", err
	}

	jarPath := filepath.Join(j.jarCache, jarName)

	if jarExists(jarPath) {
		return jarPath, nil
	}

	if strings.Contains(version, ".dev") {
		return "", fmt.Errorf("cannot pull dev versions of JARs, please run \"gradlew %v\" to start your expansion service",
			gradleTarget)
	}

	resp, err := http.Get(string(fullURL))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("received non 200 response code, got %v", resp.StatusCode)
	}

	file, err := os.Create(jarPath)
	if err != nil {
		return "", err
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}

	return jarPath, nil
}

// getURLForBeamJar builds the Maven URL for the JAR and the JAR name, returning both
// separately so the JAR name can be used for saving the file later.
func (j *jarGetter) getURLForBeamJar(gradleTarget, version string) (url, string) {
	gradlePath := strings.ReplaceAll(gradleTarget, ":", "-")
	targetPath := "beam" + gradlePath
	jarName := fmt.Sprintf("%s-%s.jar", targetPath, version)
	finalURL := j.repository + url(path.Join("/", j.groupID, targetPath, version, jarName))
	return finalURL, jarName
}

// dropEndOfGradleTarget drops the last substring off of the gradle target. This
// is used to build the Maven target and JAR name (the last substring on the gradle)
// command is usually a directive, not a reference to the desired JAR.)
func dropEndOfGradleTarget(gradleTarget string) string {
	i := strings.LastIndex(gradleTarget, ":")
	return gradleTarget[:i]
}

// jarExists checks if a file path exists/is accessible and returns true if os.Stat
// does not return an error. Does not create a file or directory if not present.
func jarExists(jarPath string) bool {
	_, err := os.Stat(jarPath)
	return err == nil
}
