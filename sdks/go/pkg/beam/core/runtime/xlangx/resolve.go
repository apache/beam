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

func Resolve(ctx context.Context, edges []*graph.MultiEdge, p *pipepb.Pipeline) {
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
