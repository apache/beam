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
// cross-language transforms.
package expansionx

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

// URL is a type used to differentiate between strings and URLs
type URL string

const (
	apacheRepository = URL("https://repo.maven.apache.org/maven2")
	beamGroupID      = "org/apache/beam"
	jarCache         = "~/.apache_beam/cache/jars"
)

// GetBeamJar checks a temporary directory for the desired Beam JAR, downloads the
// appropriate JAR from Maven if not present, then returns the file path to the
// JAR.
func GetBeamJar(gradleTarget, version string) (string, error) {
	strippedTarget := dropEndOfGradleTarget(gradleTarget)
	fullURL, jarName := getURLForBeamJar(strippedTarget, version)

	resp, err := http.Get(string(fullURL))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("Received non 200 response code, got %v", resp.StatusCode)
	}

	cacheDir := getCacheDir()
	err = checkDir(cacheDir)
	if err != nil {
		return "", err
	}

	jarPath := filepath.Join(cacheDir, jarName)

	if jarExists(jarPath) {
		return jarPath, nil
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
func getURLForBeamJar(gradleTarget, version string) (URL, string) {
	baseURL := URL(apacheRepository + "/" + beamGroupID + "/")
	fullTarget := "beam" + gradleTarget
	targetPath := strings.ReplaceAll(fullTarget, ":", "-")
	jarName := strings.ReplaceAll(fullTarget, ":", "-") + "-" + version + ".jar"
	return baseURL + URL(targetPath+"/"+version+"/"+jarName), jarName
}

// dropEndOfGradleTarget drops the last substring off of the gradle target. This
// is used to build the Maven target and JAR name (the last substring on the gradle)
// command is usually a directive, not a reference to the desired JAR.)
func dropEndOfGradleTarget(gradleTarget string) string {
	elms := strings.Split(gradleTarget, ":")
	droppedSuffix := elms[:len(elms)-1]
	return strings.Join(droppedSuffix, ":")
}

// getCacheDir returns the absolute file path of the JAR cache from the user's HOME
// directory.
func getCacheDir() string {
	usr, _ := user.Current()
	return filepath.Join(usr.HomeDir, jarCache[2:])
}

// checkDir checks that a given directory exists, then creates the directory and
// parent directories if it does not. Returns another error if the returned
// error from os.Stat is not an ErrNotExist error.
func checkDir(dirPath string) error {
	_, err := os.Stat(dirPath)
	if err == nil {
		return nil
	} else if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dirPath, 0700)
	} else {
		return err
	}
}

// jarExists checks if a file path exists/is accessible and returns true if os.Stat
// does not return an error. Does not create a file or directory if not present.
func jarExists(jarPath string) bool {
	_, err := os.Stat(jarPath)
	return err == nil
}
