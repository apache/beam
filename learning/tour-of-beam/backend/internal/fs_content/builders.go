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

package fs_content

import (
	"fmt"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
)

const OriginTbExamples = "TB_EXAMPLES"

// Construct example/snippet_id from (sdk, name)
// according to the convention defined
// in Playground datastore
func makeTbSnippetId(sdk tob.Sdk, name string) string {
	// leave blank if not defined
	if name == "" {
		return ""
	}
	return fmt.Sprintf("%s_%s_%s", OriginTbExamples, sdk.StorageID(), name)
}

type UnitBuilder struct {
	tob.Unit
}

func NewUnitBuilder(info learningUnitInfo, sdk tob.Sdk) UnitBuilder {
	taskSnippetId := makeTbSnippetId(sdk, info.TaskName)
	solutionSnippetId := makeTbSnippetId(sdk, info.SolutionName)
	return UnitBuilder{tob.Unit{
		Id:                info.Id,
		Title:             info.Name,
		TaskSnippetId:     taskSnippetId,
		SolutionSnippetId: solutionSnippetId,
	}}
}

func (b *UnitBuilder) AddDescription(d string) {
	b.Description = d
}

func (b *UnitBuilder) AddHint(h string) {
	b.Hints = append(b.Hints, h)
}

func (b *UnitBuilder) Build() *tob.Unit {
	return &b.Unit
}
