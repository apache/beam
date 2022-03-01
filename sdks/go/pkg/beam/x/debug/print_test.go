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

package debug

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestPrint(t *testing.T) {
	p, s, sequence := ptest.CreateList([]string{"abc", "def", "ghi"})
	Print(s, sequence)

	output := captureRunLogging(p)
	if !strings.Contains(output, "Elm: abc") {
		t.Errorf("Print() should contain \"Elm: abc\", got: %v", output)
	}
	if !strings.Contains(output, "Elm: def") {
		t.Errorf("Print() should contain \"Elm: def\", got: %v", output)
	}
	if !strings.Contains(output, "Elm: ghi") {
		t.Errorf("Print() should contain \"Elm: ghi\", got: %v", output)
	}
}

func TestPrintf(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	sequence := beam.Create(s, "abc", "def", "ghi")
	Printf(s, "myformatting - %v", sequence)

	output := captureRunLogging(p)
	if !strings.Contains(output, "myformatting - abc") {
		t.Errorf("Printf() should contain \"myformatting - abc\", got: %v", output)
	}
	if !strings.Contains(output, "myformatting - def") {
		t.Errorf("Printf() should contain \"myformatting - def\", got: %v", output)
	}
	if !strings.Contains(output, "myformatting - ghi") {
		t.Errorf("Printf() should contain \"myformatting - ghi\", got: %v", output)
	}
}

func TestPrint_KV(t *testing.T) {
	p, s, sequence := ptest.CreateList([]string{"abc", "def", "ghi"})
	kvSequence := beam.AddFixedKey(s, sequence)
	Print(s, kvSequence)

	output := captureRunLogging(p)
	if !strings.Contains(output, "Elm: (0,abc)") {
		t.Errorf("Print() should contain \"Elm: (0,abc)\", got: %v", output)
	}
	if !strings.Contains(output, "Elm: (0,def)") {
		t.Errorf("Print() should contain \"Elm: (0,def)\", got: %v", output)
	}
	if !strings.Contains(output, "Elm: (0,ghi)") {
		t.Errorf("Print() should contain \"Elm: (0,ghi)\", got: %v", output)
	}
}

func TestPrint_CoGBK(t *testing.T) {
	p, s, sequence := ptest.CreateList([]string{"abc", "def", "ghi"})
	kvSequence := beam.AddFixedKey(s, sequence)
	gbkSequence := beam.CoGroupByKey(s, kvSequence)
	Print(s, gbkSequence)

	output := captureRunLogging(p)
	if !strings.Contains(output, "Elm: (0,[abc def ghi])") {
		t.Errorf("Print() should contain \"Elm: (0,[abc def ghi])\", got: %v", output)
	}
}

func captureRunLogging(p *beam.Pipeline) string {
	// Pipe output to out
	var out bytes.Buffer
	log.SetOutput(&out)

	ptest.Run(p)

	// Return to original state
	log.SetOutput(os.Stderr)
	return out.String()
}
