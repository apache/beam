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
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
)

// Stage stages the worker binary and any additional files to the given
// artifact staging endpoint. It returns the retrieval token if successful.
func Stage(ctx context.Context, id, endpoint, binary, st string, files ...artifact.KeyedFile) (retrievalToken string, err error) {
	ctx = grpcx.WriteWorkerID(ctx, id)
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return "", errors.WithContext(err, "connecting to artifact service")
	}
	defer cc.Close()

	client := jobpb.NewArtifactStagingServiceClient(cc)

	files = append(files, artifact.KeyedFile{Key: "worker", Filename: binary})

	md, err := artifact.MultiStage(ctx, client, 10, files, st)
	if err != nil {
		return "", errors.WithContext(err, "staging artifacts")
	}
	token, err := artifact.Commit(ctx, client, md, st)
	if err != nil {
		return "", errors.WithContext(err, "committing artifacts")
	}
	return token, nil
}
