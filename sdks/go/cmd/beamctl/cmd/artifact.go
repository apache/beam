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

package cmd

import (
	"path/filepath"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	pb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/spf13/cobra"
)

var (
	artifactCmd = &cobra.Command{
		Use:   "artifact",
		Short: "Artifact commands",
	}

	stageCmd = &cobra.Command{
		Use:   "stage",
		Short: "Stage local files as artifacts",
		RunE:  stageFn,
		Args:  cobra.MinimumNArgs(1),
	}

	listCmd = &cobra.Command{
		Use:   "list",
		Short: "List artifacts",
		RunE:  listFn,
		Args:  cobra.NoArgs,
	}
)

func init() {
	artifactCmd.AddCommand(stageCmd, listCmd)
}

func stageFn(cmd *cobra.Command, args []string) error {
	ctx, cc, err := dial()
	if err != nil {
		return err
	}
	defer cc.Close()

	// (1) Use flat filename as key.

	var files []artifact.KeyedFile
	for _, arg := range args {
		files = append(files, artifact.KeyedFile{Key: filepath.Base(arg), Filename: arg})
	}

	// (2) Stage files in parallel, commit and print out token

	client := pb.NewArtifactStagingServiceClient(cc)
	list, err := artifact.MultiStage(ctx, client, 10, files)
	if err != nil {
		return err
	}
	token, err := artifact.Commit(ctx, client, list)
	if err != nil {
		return err
	}

	cmd.Println(token)
	return nil
}

func listFn(cmd *cobra.Command, args []string) error {
	ctx, cc, err := dial()
	if err != nil {
		return err
	}
	defer cc.Close()

	client := pb.NewArtifactRetrievalServiceClient(cc)
	md, err := client.GetManifest(ctx, &pb.GetManifestRequest{})
	if err != nil {
		return err
	}

	for _, a := range md.GetManifest().GetArtifact() {
		cmd.Println(a.Name)
	}
	return nil
}
