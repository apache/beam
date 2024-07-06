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

package dot

import (
	"bytes"
	"fmt"
	"regexp"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
)

func TestRenderPipeline(t *testing.T) {
	ptransform1 := &pipeline_v1.PTransform{
		UniqueName: "ptransform1",
		Inputs: map[string]string{
			"id1": "pcollection1",
			"id2": "pcollection2",
		},
		Outputs: map[string]string{
			"id3": "pcollection3",
			"id4": "pcollection4",
		},
	}
	pcollection1 := &pipeline_v1.PCollection{
		UniqueName: "pcollection1",
	}
	pcollection2 := &pipeline_v1.PCollection{
		UniqueName: "pcollection2",
	}
	pcollection3 := &pipeline_v1.PCollection{
		UniqueName: "pcollection3",
	}
	pcollection4 := &pipeline_v1.PCollection{
		UniqueName: "pcollection4",
	}
	p := &pipeline_v1.Pipeline{
		Components: &pipeline_v1.Components{
			Transforms: map[string]*pipeline_v1.PTransform{
				"TransformScope1": ptransform1,
			},
			Pcollections: map[string]*pipeline_v1.PCollection{
				"PCollectionScope1": pcollection1,
				"PCollectionScope2": pcollection2,
				"PCollectionScope3": pcollection3,
				"PCollectionScope4": pcollection4,
			},
		},
	}

	// Render the above pipeline_v1.Pipeline to DOT compatible representation.
	buf := new(bytes.Buffer)
	RenderPipeline(p, buf)

	// Populate the result we want and the result we got.
	want := `digraph execution_plan {
		label="execution_plan"
		labeljust="l";
		fontname="Ubuntu";
		fontsize="13";
		bgcolor="lightgray";
		style="solid";
		penwidth="0.5";
		concentrate="true";
	  
		// Node definition used for multiedge
		node [shape="rectangle" style="filled" fillcolor="honeydew" fontname="Ubuntu" penwidth="1.0" margin="0.05,0.0.05"];
	  
		bgcolor="#e6ecfa";
		"ptransform1" [ shape="ellipse" fillcolor = "lightblue" label="PTransform"]
		"pcollection1" [ shape="ellipse" fillcolor = "lightblue" label="PCollection"]
		"pcollection2" [ shape="ellipse" fillcolor = "lightblue" label="PCollection"]
		"pcollection3" [ shape="ellipse" fillcolor = "lightblue" label="PCollection"]
		"pcollection4" [ shape="ellipse" fillcolor = "lightblue" label="PCollection"]
		"pcollection1->ptransform1" [ label="PCollection to PTransform" ]
		"pcollection1" -> "ptransform1"
		"pcollection2->ptransform1" [ label="PCollection to PTransform" ]
		"pcollection2" -> "ptransform1"
		"ptransform1->pcollection3" [ label="PTransform to PCollection" ]
		"ptransform1" -> "pcollection3"
		"ptransform1->pcollection4" [ label="PTransform to PCollection" ]
		"ptransform1" -> "pcollection4"
	  
	  }`
	got := fmt.Sprintf(`%v`, buf.String())

	// Remove all whitespaces and newlines to compare the strings.
	re := regexp.MustCompile(`[\s\n]+`)
	want, got = re.ReplaceAllString(want, ""), re.ReplaceAllString(got, "")
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("diff: %v", diff)
	}
}
