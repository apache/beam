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

package runnerlib

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
)

// Stage stages the worker binary and any additional content to the given endpoint.
// It returns the commit token if successful.
func Stage(ctx context.Context, id, endpoint, binary string, files ...artifact.KeyedFile) (string, error) {
	ctx = grpcx.WriteWorkerID(ctx, id)
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return "", fmt.Errorf("failed to connect to artifact service: %v", err)
	}
	defer cc.Close()

	client := jobpb.NewArtifactStagingServiceClient(cc)

	files = append(files, artifact.KeyedFile{Key: "worker", Filename: binary})

	md, err := artifact.MultiStage(ctx, client, 10, files)
	if err != nil {
		return "", fmt.Errorf("failed to stage artifacts: %v", err)
	}
	token, err := artifact.Commit(ctx, client, md)
	if err != nil {
		return "", fmt.Errorf("failed to commit artifacts: %v", err)
	}
	return token, nil
}
