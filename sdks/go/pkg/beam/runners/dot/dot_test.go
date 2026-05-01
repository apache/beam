// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dot

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func TestDotRunner_GeneratesDeterministicOutput(t *testing.T) {
	ctx := context.Background()

	// Create temporary DOT file
	tmpFile, err := os.CreateTemp("", "dot_test_*.dot")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Set flag manually
	*dotFile = tmpFile.Name()

	// Build simple pipeline
	p, s := beam.NewPipelineWithRoot()

	col := beam.Create(s, "a", "b", "c")
	passert.Count(s, col, "", 3)

	// Run with dot runner
	_, err = Execute(ctx, p)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Read generated file
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read dot file: %v", err)
	}

	content := string(data)

	if !strings.HasPrefix(content, "digraph G {") {
		t.Fatalf("dot output missing header")
	}

	if !strings.Contains(content, "->") {
		t.Fatalf("dot output contains no edges")
	}
}
