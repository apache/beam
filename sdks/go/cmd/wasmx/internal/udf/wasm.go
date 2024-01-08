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

package udf

import (
	_ "embed"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
)

const (
	// TODO: Replace with proto const after https://github.com/apache/beam/pull/29898
	wasmUrn           = "beam:dofn:wasm:1.0"
	WasmEnvironmentId = "wasm"
)

var (
	beamInputP  = regexp.MustCompile(`//beam:input (?P<input>([a-zA-Z0-9_]+:?)+)`)
	inputKey    = "input"
	beamOutputP = regexp.MustCompile(`//beam:output (?P<output>([a-zA-Z0-9_]+:?)+)`)
	outputKey   = "output"
	exportP     = regexp.MustCompile(`//export (?P<export>([a-zA-Z0-9]+))`)
	exportKey   = "export"
)

//go:embed add/add.wasm
var addWasm WasmFn

//go:embed add/add.go
var addSrc WasmSource

//go:embed wordcount/wordcount.wasm
var wordCountWasm WasmFn

//go:embed wordcount/wordcount.go
var wordCountSrc WasmSource

// WasmFn is a wasm compiled binary with the following expectations:
// 1. It must have one ProcessElement exported method
// 2. The code is compatible with the https://extism.org/ framework
type WasmFn []byte

// FunctionSpec builds a pipeline_v1.FunctionSpec based on the wasm binary content.
func (fn WasmFn) FunctionSpec() *pipeline_v1.FunctionSpec {
	pardo := &pipeline_v1.ParDoPayload{
		DoFn: &pipeline_v1.FunctionSpec{
			Urn:     wasmUrn,
			Payload: fn,
		},
	}
	return &pipeline_v1.FunctionSpec{
		Urn:     graphx.URNParDo,
		Payload: protox.MustEncode(pardo),
	}
}

// WasmSource is source code that can be compiled into wasm for use with
// the https://extism.org/ framework.
// It is expected to have the following:
//  1. It must have one ProcessElement exported method
//  2. It must decode input and encode output compatible with the https://extism.org/ framework
//  3. It must have a comments in the file specifying Beam coders for input and output.
//     Go source for example would have:
//     //beam:input beam:coder:varint:v1
//     //beam:output beam:coder:varint:v1
type WasmSource []byte

// WasmSourceMetadata contains metadata related to wasm source code.
type WasmSourceMetadata struct {
	BeamInput  string
	BeamOutput string
	Exports    []string
}

func (meta *WasmSourceMetadata) isValidErr() error {
	var missing []string
	if meta.BeamInput == "" {
		missing = append(missing, "//beam:input")
	}
	if meta.BeamOutput == "" {
		missing = append(missing, "//beam:output")
	}
	if len(meta.Exports) == 0 {
		missing = append(missing, "//export")
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing the expected code metadata code comments: %s", strings.Join(missing, ", "))
	}

	return nil
}

func tryMatchAssign(p *regexp.Regexp, key string, dst *string, src string) {
	if !p.MatchString(src) {
		return
	}
	matches := p.FindStringSubmatch(src)
	index := p.SubexpIndex(key)
	*dst = matches[index]
}

func tryMatchAssignSlice(p *regexp.Regexp, key string, dst *[]string, src string) {
	var result string
	tryMatchAssign(p, key, &result, src)
	if result != "" {
		*dst = append(*dst, result)
	}
}

func Parse(src WasmSource) (*WasmSourceMetadata, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", ([]byte)(src), parser.ParseComments)
	if err != nil {
		return nil, err
	}
	result := &WasmSourceMetadata{}
	for _, grp := range f.Comments {
		for _, cmt := range grp.List {
			if result.BeamInput == "" {
				tryMatchAssign(beamInputP, inputKey, &result.BeamInput, cmt.Text)
			}
			if result.BeamOutput == "" {
				tryMatchAssign(beamOutputP, outputKey, &result.BeamOutput, cmt.Text)
			}
			tryMatchAssignSlice(exportP, exportKey, &result.Exports, cmt.Text)
		}
	}
	return result, err
}
