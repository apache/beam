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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetURLForBeamJar(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		version     string
		expectedURL URL
		expectedJAR string
	}{
		{
			"base test",
			":sdks:java:fake",
			"VERSION",
			"https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-fake/VERSION/",
			"beam-sdks-java-fake-VERSION.jar",
		},
		{
			"versioned test",
			":sdks:java:fake",
			"1.2.3",
			"https://repo.maven.apache.org/maven2/org/apache/beam/beam-sdks-java-fake/1.2.3/",
			"beam-sdks-java-fake-1.2.3.jar",
		},
	}
	for _, test := range tests {
		madeURL, madeJAR := getURLForBeamJar(test.target, test.version)
		if madeURL != test.expectedURL {
			t.Errorf("test %v failed: wanted URL %v, got %v", test.name, test.expectedURL, madeURL)
		}

		if madeJAR != test.expectedJAR {
			t.Errorf("test %v failed: wanted JAR %v, got %v", test.name, test.expectedJAR, madeJAR)
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

func TestGetCacheDir(t *testing.T) {
	cacheDir := getCacheDir()
	if !strings.Contains(cacheDir, jarCache[2:]) {
		t.Errorf("failed to get cache directory: wanted %v, got %v", jarCache[:2], cacheDir)
	}
}

func TestCheckDir(t *testing.T) {
	d, err := ioutil.TempDir(os.Getenv("TEST_TMPDIR"), "expansionx-*")
	if err != nil {
		t.Fatalf("failed to make temp directory, got %v", err)
	}
	defer os.RemoveAll(d)

	err = checkDir(d)
	if err != nil {
		t.Errorf("checkDir returned error, got %v", err)
	}
}

func TestCheckDir_create(t *testing.T) {
	d := filepath.Join(os.Getenv("TEST_TMPDIR"), "expansion-test")
	_, err := os.Stat(d)
	if err == nil {
		t.Errorf("Temp directory already exists when it shouldn't")
	}

	err = checkDir(d)
	if err != nil {
		t.Errorf("checkDir returned an error, got %v", err)
	}
	defer os.RemoveAll(d)

	_, err = os.Stat(d)
	if err != nil {
		t.Errorf("Temp directory check returned error, got %v", err)
	}
}

func TestJarExists(t *testing.T) {
	d, err := ioutil.TempDir(os.Getenv("TEST_TMPDIR"), "expansionx-*")
	if err != nil {
		t.Fatalf("failed to make temp directory, got %v", err)
	}
	defer os.RemoveAll(d)

	tmpFile, err := ioutil.TempFile(d, "expansion-*.jar")
	if err != nil {
		t.Fatalf("failed to make temp file, got %v", err)
	}

	if !jarExists(tmpFile.Name()) {
		t.Errorf("jarExists returned unexpected value for path %v, wanted true, got false", tmpFile.Name())
	}
}

func TestJarExists_bad(t *testing.T) {
	d, err := ioutil.TempDir(os.Getenv("TEST_TMPDIR"), "expansionx-*")
	if err != nil {
		t.Fatalf("failed to make temp directory, got %v", err)
	}
	defer os.RemoveAll(d)

	fakePath := filepath.Join(d, "not-a-file.jar")

	if jarExists(fakePath) {
		t.Errorf("jarExists returned unexpected value for path %v, wanted false, got true", fakePath)
	}
}
