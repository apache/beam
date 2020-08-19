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

package graph

import (
	"math/rand"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

var (
	SourceInputTag string
	SinkOutputTag  string
	NewNamespace   func() string
)

type ExpandedTransform struct {
	Components   interface{} // *pipepb.Components
	Transform    interface{} //*pipepb.PTransform
	Requirements []string
}

// ExternalTransform represents the cross-language transform in and out of the Pipeline as a MultiEdge and Expanded proto respectively
type ExternalTransform struct {
	Urn           string
	Payload       []byte
	ExpansionAddr string

	InputsMap  map[string]int
	OutputsMap map[string]int

	Expanded *ExpandedTransform
}

func init() {
	NewNamespace = NewNamespaceGenerator(10)
	SourceInputTag = NewNamespace()
	SinkOutputTag = NewNamespace()
}

func (ext ExternalTransform) WithNamedInputs(inputsMap map[string]int) ExternalTransform {
	if ext.InputsMap != nil {
		panic(errors.Errorf("inputs already set as: \n%v", ext.InputsMap))
	}
	ext.InputsMap = inputsMap
	return ext
}

func (ext ExternalTransform) WithNamedOutputs(outputsMap map[string]int) ExternalTransform {
	if ext.OutputsMap != nil {
		panic(errors.Errorf("outputTypes already set as: \n%v", ext.OutputsMap))
	}
	ext.OutputsMap = outputsMap
	return ext
}

// TODO(pskevin): Credit one of the best stackoverflow answers @ https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go

func NewNamespaceGenerator(n int) func() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIDBits = 6                   // 6 bits to represent a letter index
		letterIDMask = 1<<letterIDBits - 1 // All 1-bits, as many as letterIDBits
		letterIDMax  = 63 / letterIDBits   // # of letter indices fitting in 63 bits
	)

	var src = rand.NewSource(time.Now().UnixNano())

	random := func() string {
		sb := strings.Builder{}
		sb.Grow(n)
		// A src.Int63() generates 63 random bits, enough for letterIDMax characters!
		for i, cache, remain := n-1, src.Int63(), letterIDMax; i >= 0; {
			if remain == 0 {
				cache, remain = src.Int63(), letterIDMax
			}
			if idx := int(cache & letterIDMask); idx < len(letters) {
				sb.WriteByte(letters[idx])
				i--
			}
			cache >>= letterIDBits
			remain--
		}

		return sb.String()
	}

	return random
}
