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

package xlangx

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/protox"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
)

// ResolveArtifacts acquires all dependencies for a cross-language transform
func ResolveArtifacts(ctx context.Context, edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	path, err := filepath.Abs("/tmp/artifacts")
	if err != nil {
		panic(err)
	}
	for _, e := range edges {
		if e.Op == graph.External {
			envs := graphx.ExpandedComponents(e.External.Expanded).Environments
			for eid, env := range envs {

				if strings.HasPrefix(eid, "go") {
					continue
				}
				deps := env.GetDependencies()
				resolvedMeta, err := artifact.Materialize(ctx, e.External.ExpansionAddr, deps, "", path)
				if err != nil {
					panic(err)
				}

				var resolvedDeps []*pipepb.ArtifactInformation
				for _, meta := range resolvedMeta {
					fullPath := filepath.Join(path, "/", meta.Name)
					resolvedDeps = append(resolvedDeps,
						&pipepb.ArtifactInformation{
							TypeUrn: "beam:artifact:type:file:v1",
							TypePayload: protox.MustEncode(
								&pipepb.ArtifactFilePayload{
									Path:   fullPath,
									Sha256: meta.Sha256,
								},
							),
							RoleUrn: graphx.URNArtifactStagingTo,
						},
					)
				}
				env.Dependencies = resolvedDeps
			}
		}
	}
}
