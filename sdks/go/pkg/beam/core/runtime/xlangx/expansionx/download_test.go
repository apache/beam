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

package expansionx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetAndSetRepositoryURL(t *testing.T) {
	tests := []struct {
		name    string
		newRepo string
		expRepo string
	}{
		{
			"correct URL",
			"http://new.repo.org",
			"http://new.repo.org",
		},
		{
			"correct URL https",
			"https://new.repo.org",
			"https://new.repo.org",
		},
		{
			"correct URL with trailing backslash",
			"http://new.repo.org/",
			"http://new.repo.org",
		},
		{
			"correct URL https with trailing backslash",
			"https://new.repo.org/",
			"https://new.repo.org",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			j := newJarGetter()
			err := j.setRepositoryURL(test.newRepo)
			if err != nil {
				t.Errorf("failed to set repository URL, got %v", err)
			}
			if got, want := j.getRepositoryURL(), test.expRepo; got != want {
				t.Errorf("getRepositoryURL() got %v, want %v", got, want)
			}
		})
	}
}

func TestGetAndSetRepositoryURL_bad(t *testing.T) {
	tests := []struct {
		name    string
		newRepo string
	}{
		{
			"no http",
			"new.maven.repo.com",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			j := newJarGetter()
			err := j.setRepositoryURL(test.newRepo)
			if err == nil {
				t.Errorf("setRepositoryURL(%v) succeeded when it should have failed", test.newRepo)
			}
			// Check that the failed Set call did not change the URL.
			if got, want := j.getRepositoryURL(), string(apacheRepository); got != want {
				t.Errorf("getRepositoryURL() got %v, want %v", got, want)
			}
		})
	}
}

func TestGetURLForBeamJar(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		version     string
		expectedURL url
	}{
		{
			"base",
			":sdks:java:fake",
			"VERSION",
			"https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-fake/VERSION/beam-sdks-java-fake-VERSION.jar",
		},
		{
			"versioned",
			":sdks:java:fake",
			"1.2.3",
			"https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-fake/1.2.3/beam-sdks-java-fake-1.2.3.jar",
		},
	}
	for _, test := range tests {
		j := newJarGetter()
		madeURL, _ := j.getURLForBeamJar(test.target, test.version)
		if madeURL != test.expectedURL {
			t.Errorf("test %v failed: wanted URL %v, got %v", test.name, test.expectedURL, madeURL)
		}
	}
}

func TestDropEndOfGradleTarget(t *testing.T) {
	target := ":sdks:java:fake:runFake"
	expected := ":sdks:java:fake"
	returned := dropEndOfGradleTarget(target)
	if returned != expected {
		t.Errorf("wanted %v, got %v", expected, returned)
	}
}

func TestNewJarGetter(t *testing.T) {
	j := newJarGetter()
	if j.repository != apacheRepository {
		t.Errorf("failed to get correct JAR repository: wanted %v, got %v", apacheRepository, j.repository)
	}
	if j.groupID != beamGroupID {
		t.Errorf("failed to get correct group ID: wanted %v, got %v", beamGroupID, j.groupID)
	}
	if !strings.Contains(j.jarCache, jarCache[2:]) {
		t.Errorf("failed to get correct cache directory: wanted %v, got %v", jarCache[:2], j.jarCache)
	}
}

func makeTempDir(t *testing.T) string {
	d, err := os.MkdirTemp(os.Getenv("TEST_TMPDIR"), "expansionx-*")
	if err != nil {
		t.Fatalf("failed to make temp directory, got %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestJarExists(t *testing.T) {
	d := makeTempDir(t)

	tmpFile, err := os.CreateTemp(d, "expansion-*.jar")
	if err != nil {
		t.Fatalf("failed to make temp file, got %v", err)
	}

	if !jarExists(tmpFile.Name()) {
		t.Errorf("jarExists returned unexpected value for path %v, wanted true, got false", tmpFile.Name())
	}
}

func TestJarExists_bad(t *testing.T) {
	d := makeTempDir(t)

	fakePath := filepath.Join(d, "not-a-file.jar")

	if jarExists(fakePath) {
		t.Errorf("jarExists returned unexpected value for path %v, wanted false, got true", fakePath)
	}
}

func getGeneratedNumberInFile(fileName, jarPrefix string) string {
	tmpFileNameSplit := strings.Split(fileName, string(filepath.Separator))
	tmpFileName := tmpFileNameSplit[len(tmpFileNameSplit)-1]
	numSuffix := strings.TrimPrefix(tmpFileName, jarPrefix)
	return strings.TrimSuffix(numSuffix, ".jar")
}

func TestGetJar_present(t *testing.T) {
	d := makeTempDir(t)

	j := newJarGetter()
	j.jarCache = d

	gradleTarget := ":sdks:java:fake:fakeJar"

	jarName := "beam-sdks-java-fake-"

	tmpFile, err := os.CreateTemp(d, jarName+"*.jar")
	if err != nil {
		t.Fatalf("failed to create temp JAR file, got %v", err)
	}

	genNumber := getGeneratedNumberInFile(tmpFile.Name(), jarName)

	fullJarName := jarName + genNumber + ".jar"
	expJarPath := filepath.Join(d, fullJarName)

	jarPath, err := j.getJar(gradleTarget, genNumber)
	if err != nil {
		t.Errorf("getJar returned error when it should have succeeded, got %v", err)
	}
	if jarPath != expJarPath {
		t.Errorf("Jar path mismatch: wanted %v, got %v", expJarPath, jarPath)
	}
}

func TestGetJar_dev(t *testing.T) {
	gradleTarget := ":sdks:java:jake:runJar"
	_, err := GetBeamJar(gradleTarget, "1.2.3.dev")
	if err == nil {
		t.Fatal("GetBeamJar succeeded when it should have failed")
	}
	if !strings.Contains(err.Error(), gradleTarget) {
		t.Errorf("error message does not contain gradle command %v for user, got message: %v", gradleTarget, err)
	}
}
