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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/protox"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
)

// ResolveArtifacts acquires all dependencies for a cross-language transform
func ResolveArtifacts(ctx context.Context, edges []*graph.MultiEdge, p *pipepb.Pipeline) {
	_, err := ResolveArtifactsWithConfig(ctx, edges, ResolveConfig{})
	if err != nil {
		panic(err)
	}
}

// ResolveConfig contains fields for configuring the behavior for resolving
// artifacts.
type ResolveConfig struct {
	// SdkPath replaces the default filepath for dependencies, but only in the
	// external environment proto to be used by the SDK Harness during pipeline
	// execution. This is used to specify alternate staging directories, such
	// as for staging artifacts remotely.
	//
	// Setting an SdkPath does not change staging behavior otherwise. All
	// artifacts still get staged to the default local filepath, and it is the
	// user's responsibility to stage those local artifacts to the SdkPath.
	SdkPath string

	// JoinFn is a function for combining SdkPath and individual artifact names.
	// If not specified, it defaults to using filepath.Join.
	JoinFn func(path, name string) string
}

func defaultJoinFn(path, name string) string {
	return filepath.Join(path, "/", name)
}

// ResolveArtifactsWithConfig acquires all dependencies for cross-language
// transforms, but with some additional configuration to behavior. By default,
// this function performs the following steps for each cross-language transform
// in the list of edges:
//  1. Retrieves a list of dependencies needed from the expansion service.
//  2. Retrieves each dependency as an artifact and stages it to a default
//     local filepath.
//  3. Adds the dependencies to the transform's stored environment proto.
//
// The changes that can be configured are documented in ResolveConfig.
//
// This returns a map of "local path" to "sdk path". By default these are
// identical, unless ResolveConfig.SdkPath has been set.
func ResolveArtifactsWithConfig(ctx context.Context, edges []*graph.MultiEdge, cfg ResolveConfig) (paths map[string]string, err error) {
	tmpPath, err := filepath.Abs("/tmp/artifacts")
	if err != nil {
		return nil, errors.WithContext(err, "resolving remote artifacts")
	}
	if cfg.JoinFn == nil {
		cfg.JoinFn = defaultJoinFn
	}
	paths = make(map[string]string)
	for _, e := range edges {
		if e.Op == graph.External && e.External != nil {
			components, err := graphx.ExpandedComponents(e.External.Expanded)
			if err != nil {
				return nil, errors.WithContextf(err,
					"resolving remote artifacts for edge %v", e.Name())
			}
			envs := components.Environments
			for eid, env := range envs {
				if strings.HasPrefix(eid, "go") {
					continue
				}
				if strings.HasPrefix(e.External.ExpansionAddr, autoJavaNamespace) {
					continue
				}
				deps := env.GetDependencies()
				resolvedArtifacts, err := artifact.Materialize(ctx, e.External.ExpansionAddr, deps, "", tmpPath)
				if err != nil {
					return nil, errors.WithContextf(err,
						"resolving remote artifacts for env %v in edge %v", eid, e.Name())
				}

				var resolvedDeps []*pipepb.ArtifactInformation
				for _, a := range resolvedArtifacts {
					name, sha256 := artifact.MustExtractFilePayload(a)
					fullTmpPath := filepath.Join(tmpPath, "/", name)
					fullSdkPath := fullTmpPath
					if len(cfg.SdkPath) > 0 {
						fullSdkPath = cfg.JoinFn(cfg.SdkPath, name)
					}
					resolvedDeps = append(resolvedDeps,
						&pipepb.ArtifactInformation{
							TypeUrn: "beam:artifact:type:file:v1",
							TypePayload: protox.MustEncode(
								&pipepb.ArtifactFilePayload{
									Path:   fullSdkPath,
									Sha256: sha256,
								},
							),
							RoleUrn:     a.RoleUrn,
							RolePayload: a.RolePayload,
						},
					)
					paths[fullTmpPath] = fullSdkPath
				}
				env.Dependencies = resolvedDeps
			}
		}
	}
	return paths, nil
}

// UpdateArtifactTypeFromFileToURL changes the type of the artifact from FILE to URL
// when the file path contains the suffix element ("://") of the URI scheme.
func UpdateArtifactTypeFromFileToURL(edges []*graph.MultiEdge) {
	for _, e := range edges {
		if e.Op == graph.External && e.External != nil {
			components, err := graphx.ExpandedComponents(e.External.Expanded)
			if err != nil {
				panic(errors.WithContextf(err,
					"updating URL artifacts type for edge %v", e.Name()))
			}
			envs := components.Environments
			for _, env := range envs {
				deps := env.GetDependencies()
				var resolvedDeps []*pipepb.ArtifactInformation
				for _, a := range deps {
					path, sha256 := artifact.MustExtractFilePayload(a)
					if strings.Contains(path, "://") {
						a.TypeUrn = "beam:artifact:type:url:v1"
						a.TypePayload = protox.MustEncode(
							&pipepb.ArtifactUrlPayload{
								Url:    path,
								Sha256: sha256,
							},
						)
					}
					resolvedDeps = append(resolvedDeps, a)
				}
				env.Dependencies = resolvedDeps
			}
		}
	}
}
