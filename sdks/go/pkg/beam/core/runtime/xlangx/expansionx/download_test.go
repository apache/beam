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
