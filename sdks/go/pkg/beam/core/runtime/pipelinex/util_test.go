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

package pipelinex

import (
	"fmt"
	"testing"

	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
)

func TestTopologicalSort(t *testing.T) {
	graphs := map[string]map[string]*pipepb.PTransform{
		"linkedList": map[string]*pipepb.PTransform{
			"p0": ptImpulse("n0"),
			"p1": ptNoSide("n0", "n1"),
			"p2": ptNoSide("n1", "n2"),
			"p3": ptNoSide("n2", "n3"),
			"p4": ptSink("n3"),
		},
		"binarytree": map[string]*pipepb.PTransform{
			"p0":   ptImpulse("n0"),
			"p1a":  ptNoSide("n0", "n1a"),
			"p1b":  ptNoSide("n0", "n1b"),
			"p2aa": ptSink("n1a"),
			"p2ab": ptSink("n1a"),
			"p2ba": ptSink("n1b"),
			"p2bb": ptSink("n1b"),
		},
		"binarytreeWComps": map[string]*pipepb.PTransform{
			"p0":   ptImpulse("n0"),
			"p1a":  ptNoSide("n0", "n1a"),
			"p1b":  ptNoSide("n0", "n1b"),
			"p2aa": ptSink("n1a"),
			"p2ab": ptSink("n1a"),
			"p2ba": ptSink("n1b"),
			"p2bb": ptSink("n1b"),
			"c1":   ptComp("p0", "p1a", "p1b"),
			"c2":   ptComp("p2aa", "p2ab"),
			"c3":   ptComp("p2ba", "p2bb"),
		},
		"linkedListWComps": map[string]*pipepb.PTransform{
			"p0": ptImpulse("n0"),
			"p1": ptNoSide("n0", "n1"),
			"p2": ptNoSide("n1", "n2"),
			"p3": ptNoSide("n2", "n3"),
			"p4": ptSink("n3"),
			"c1": ptComp("p0"),
			"c2": ptComp("p1", "p2"),
			"c3": ptComp("c1", "c2", "p3"),
		},
		"leafdag": map[string]*pipepb.PTransform{
			"A":  ptImpulse("n0"),
			"B":  ptNoSide("n0", "n1"),
			"C":  ptSink("n0", "n1"),
			"c1": ptComp("A", "C"),
			"c2": ptComp("B"),
		},
	}

	for k, g := range graphs {
		graphs[k] = computeCompositeInputOutput(g)
	}

	tests := []struct {
		graph  string
		toSort []string
	}{
		{
			graph:  "linkedList",
			toSort: []string{},
		}, {
			graph:  "linkedList",
			toSort: []string{"p0", "p1", "p2", "p3", "p4"},
		}, {
			graph:  "linkedList",
			toSort: []string{"p3", "p4", "p0", "p1", "p2"},
		}, {
			graph:  "binarytree",
			toSort: []string{"p0", "p1a", "p1b", "p2aa", "p2ab", "p2ba", "p2bb"},
		}, {
			graph:  "binarytree",
			toSort: []string{"p0", "p2bb", "p2aa", "p2ba", "p1b", "p1a", "p2ab"},
		}, {
			graph:  "binarytree",
			toSort: []string{"p1b", "p0", "p2ba"},
		}, {
			graph:  "binarytreeWComps",
			toSort: []string{"p0", "p1a", "p1b", "p2aa", "p2ab", "p2ba", "p2bb"},
		}, {
			graph:  "binarytreeWComps",
			toSort: []string{"c1", "p2aa", "p2ab", "p2ba", "p2bb"},
		}, {
			graph:  "binarytreeWComps",
			toSort: []string{"c1", "c2", "c3"},
		}, {
			graph:  "linkedListWComps",
			toSort: []string{"c3", "p4"},
		}, {
			graph:  "linkedListWComps",
			toSort: []string{"c1", "c2", "p3", "p4"},
		}, {
			graph:  "linkedListWComps",
			toSort: []string{"p0", "c2", "p3", "p4"},
		}, {
			graph:  "linkedListWComps",
			toSort: []string{"c1", "p1", "p2", "p3", "p4"},
		}, {
			graph:  "leafdag",
			toSort: []string{"A", "B", "C"},
		}, {
			graph:  "leafdag",
			toSort: []string{"A", "c2", "C"},
		},
	}
	for _, test := range tests {
		t.Run(test.graph, func(t *testing.T) {
			xforms := graphs[test.graph]
			got1 := TopologicalSort(xforms, test.toSort)
			got2 := TopologicalSort(xforms, test.toSort)
			if diff := cmp.Diff(got1, got2); diff != "" {
				t.Errorf("TopologicalSort(%v, %v) not deterministic: %v", test.graph, test.toSort, diff)
			}
			validateSortForTest(t, xforms, got1)
		})
	}
}

func ptSink(input string, sides ...string) *pipepb.PTransform {
	ins := map[string]string{"i0": input}
	for i, s := range sides {
		ins[fmt.Sprintf("i%d", i+1)] = s
	}
	return &pipepb.PTransform{
		Inputs: ins,
	}
}

// ptComp generates a composite PCollection. Unlike the other helpers, it takes in
// the *subtransform ids* instead of the input/output pcollections.
func ptComp(subs ...string) *pipepb.PTransform {
	return &pipepb.PTransform{
		Subtransforms: subs,
	}
}

func ptImpulse(output string) *pipepb.PTransform {
	return &pipepb.PTransform{
		Outputs: map[string]string{"o0": output},
	}
}

func pt(inputs []string, outputs ...string) *pipepb.PTransform {
	ins := make(map[string]string)
	for i, in := range inputs {
		ins[fmt.Sprintf("i%d", i)] = in
	}
	outs := make(map[string]string)
	for i, out := range outputs {
		outs[fmt.Sprintf("i%d", i)] = out
	}
	return &pipepb.PTransform{
		Inputs:  ins,
		Outputs: outs,
	}
}

func ptNoSide(input string, outputs ...string) *pipepb.PTransform {
	outs := make(map[string]string)
	for i, o := range outputs {
		outs[fmt.Sprintf("o%d", i)] = o
	}
	return &pipepb.PTransform{
		Inputs:  map[string]string{"i0": input},
		Outputs: outs,
	}
}

// validateSortForTest ensures that in the sorted ids, outputs are declared before
// they're used as inputs.
// Distinct from validateSort to better communicate error details using the test harness,
// and base things on the generated unique ids, rather than the user readable scopes.
func validateSortForTest(t *testing.T, xforms map[string]*pipepb.PTransform, sorted []string) {
	t.Helper()
	seenPCollections := make(map[string]bool)
	missingPCollections := make(map[string]bool)

	for _, id := range sorted {
		pt := xforms[id]
		for _, in := range pt.Inputs {
			if !seenPCollections[in] {
				t.Errorf("out of order pcollection %v required by %v", in, id)
				missingPCollections[in] = true
			}
		}

		if len(pt.GetSubtransforms()) != 0 {
			for _, sptid := range pt.GetSubtransforms() {
				spt := xforms[sptid]
				for _, out := range spt.GetOutputs() {
					if missingPCollections[out] {
						t.Errorf("missing pcollection %v created by %v in composite %v", out, sptid, id)
					}
				}
			}
		}
		for _, out := range pt.GetOutputs() {
			seenPCollections[out] = true
			if missingPCollections[out] {
				t.Errorf("missing pcollection %v created by %v", out, id)
			}
		}
	}
	if len(missingPCollections) != 0 {
		t.Log(sorted)
	}
}
