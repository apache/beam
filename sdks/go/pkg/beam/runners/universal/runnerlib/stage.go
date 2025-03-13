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
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Stage stages the worker binary and any additional files to the given
// artifact staging endpoint. It returns the retrieval token if successful.
func Stage(ctx context.Context, id, endpoint, binary, st string) (retrievalToken string, err error) {
	ctx = grpcx.WriteWorkerID(ctx, id)
	cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	if err != nil {
		return "", errors.WithContext(err, "connecting to artifact service")
	}
	defer cc.Close()

	if err := StageViaPortableAPI(ctx, cc, binary, st); err == nil {
		return st, nil
	}
	log.Warnf(ctx, "unable to stage with PortableAPI: %v; falling back to legacy", err)

	return StageViaLegacyAPI(ctx, cc, binary, st)
}

// StageViaPortableAPI is a beam internal function for uploading artifacts to the staging service
// via the portable API.
//
// It will be unexported at a later time.
func StageViaPortableAPI(ctx context.Context, cc *grpc.ClientConn, binary, st string) (retErr error) {
	const attempts = 3
	var failures []string
	for {
		err := stageFiles(ctx, cc, binary, st)
		if err == nil {
			return nil // success!
		}
		failures = append(failures, err.Error())
		if len(failures) > attempts {
			return errors.Errorf("failed to stage artifacts for token %v in %v attempts: %v", st, attempts, strings.Join(failures, ";\n"))
		}
	}
}

func stageFiles(ctx context.Context, cc *grpc.ClientConn, binary, st string) error {
	client := jobpb.NewArtifactStagingServiceClient(cc)
	stream, err := client.ReverseArtifactRetrievalService(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := stream.CloseSend(); err != nil {
			log.Error(ctx, "StageViaPortableApi CloseSend error: ", err)
		}
	}()

	if err := stream.Send(&jobpb.ArtifactResponseWrapper{StagingToken: st}); err != nil {
		return errors.Wrapf(err, "failed to send staging token")
	}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch request := in.Request.(type) {
		case *jobpb.ArtifactRequestWrapper_ResolveArtifact:
			err = stream.Send(&jobpb.ArtifactResponseWrapper{
				Response: &jobpb.ArtifactResponseWrapper_ResolveArtifactResponse{
					ResolveArtifactResponse: &jobpb.ResolveArtifactsResponse{
						Replacements: request.ResolveArtifact.Artifacts,
					},
				}})
			if err != nil {
				return err
			}

		case *jobpb.ArtifactRequestWrapper_GetArtifact:
			switch typeUrn := request.GetArtifact.Artifact.TypeUrn; typeUrn {
			// TODO(https://github.com/apache/beam/issues/21459): Legacy Type URN. If requested, provide the binary.
			// To be removed later in 2022, once thoroughly obsolete.
			case graphx.URNArtifactGoWorker:
				if err := stageFile(binary, stream); err != nil {
					if err == io.EOF {
						continue // so we can get the real error from stream.Recv.
					}
					return errors.Wrapf(err, "failed to stage Go worker binary: %v", binary)
				}
			case graphx.URNArtifactFileType:
				typePl := pipepb.ArtifactFilePayload{}
				if err := proto.Unmarshal(request.GetArtifact.Artifact.TypePayload, &typePl); err != nil {
					return errors.Wrap(err, "failed to parse artifact file payload")
				}
				if err := stageFile(typePl.GetPath(), stream); err != nil {
					if err == io.EOF {
						continue // so we can get the real error from stream.Recv.
					}
					return errors.Wrapf(err, "failed to stage file %v", typePl.GetPath())

				}
			default:
				return errors.Errorf("request has unexpected artifact type %s", typeUrn)
			}

		default:
			return errors.Errorf("request has unexpected type %T", request)
		}
	}
}

func stageFile(filename string, stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceClient) error {
	fd, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "unable to open file %v", filename)
	}
	defer fd.Close()

	data := make([]byte, 1<<20)
	for {
		n, err := fd.Read(data)
		if n > 0 {
			sendErr := stream.Send(&jobpb.ArtifactResponseWrapper{
				Response: &jobpb.ArtifactResponseWrapper_GetArtifactResponse{
					GetArtifactResponse: &jobpb.GetArtifactResponse{
						Data: data[:n],
					},
				}})
			if sendErr == io.EOF {
				return sendErr
			}

			if sendErr != nil {
				return errors.Wrap(sendErr, "StageFile chunk send failed")
			}
		}

		if err == io.EOF {
			sendErr := stream.Send(&jobpb.ArtifactResponseWrapper{
				IsLast: true,
				Response: &jobpb.ArtifactResponseWrapper_GetArtifactResponse{
					GetArtifactResponse: &jobpb.GetArtifactResponse{},
				}})
			return sendErr
		}

		if err != nil {
			return err
		}
	}
}

// StageViaLegacyAPI is a beam internal function for uploading artifacts to the staging service
// via the legacy API.
//
// It will be unexported at a later time.
func StageViaLegacyAPI(ctx context.Context, cc *grpc.ClientConn, binary, st string) (retrievalToken string, err error) {
	client := jobpb.NewLegacyArtifactStagingServiceClient(cc)

	files := []artifact.KeyedFile{{Key: "worker", Filename: binary}}

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
