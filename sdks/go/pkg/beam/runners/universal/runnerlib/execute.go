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
	"os"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
)

// Execute executes a pipeline on the universal runner serving the given endpoint.
// Convenience function.
func Execute(ctx context.Context, p *pipepb.Pipeline, endpoint string, opt *JobOptions, async bool) (string, error) {
	// (1) Prepare job to obtain artifact staging instructions.

	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return "", errors.WithContextf(err, "connecting to job service")
	}
	defer cc.Close()
	client := jobpb.NewJobServiceClient(cc)

	prepID, artifactEndpoint, st, err := Prepare(ctx, client, p, opt)
	if err != nil {
		return "", err
	}

	log.Infof(ctx, "Prepared job with id: %v and staging token: %v", prepID, st)

	// (2) Stage artifacts.

	bin := opt.Worker
	if bin == "" {
		if self, ok := IsWorkerCompatibleBinary(); ok {
			bin = self
			log.Infof(ctx, "Using running binary as worker binary: '%v'", bin)
		} else {
			// Cross-compile as last resort.

			worker, err := BuildTempWorkerBinary(ctx)
			if err != nil {
				return "", err
			}
			defer os.Remove(worker)

			bin = worker
		}
	} else {
		log.Infof(ctx, "Using specified worker binary: '%v'", bin)
	}

	token, err := Stage(ctx, prepID, artifactEndpoint, bin, st)
	if err != nil {
		return "", err
	}

	log.Infof(ctx, "Staged binary artifact with token: %v", token)

	// (3) Submit job

	jobID, err := Submit(ctx, client, prepID, token)
	if err != nil {
		return "", err
	}

	log.Infof(ctx, "Submitted job: %v", jobID)

	// (4) Wait for completion.

	if async {
		return jobID, nil
	}
	return jobID, WaitForCompletion(ctx, client, jobID)
}
