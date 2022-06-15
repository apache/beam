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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/artifact"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
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

	if err := StageViaPortableApi(ctx, cc, binary, st); err == nil {
		return "", nil
	}

	return StageViaLegacyApi(ctx, cc, binary, st)
}

func StageViaPortableApi(ctx context.Context, cc *grpc.ClientConn, binary, st string) error {
	client := jobpb.NewArtifactStagingServiceClient(cc)

	stream, err := client.ReverseArtifactRetrievalService(context.Background())
	if err != nil {
		return err
	}

	if err := stream.Send(&jobpb.ArtifactResponseWrapper{StagingToken: st}); err != nil {
		return err
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
				if err := StageFile(binary, stream); err != nil {
					return errors.Wrap(err, "failed to stage Go worker binary")
				}
			case graphx.URNArtifactFileType:
				typePl := pipepb.ArtifactFilePayload{}
				if err := proto.Unmarshal(request.GetArtifact.Artifact.TypePayload, &typePl); err != nil {
					return errors.Wrap(err, "failed to parse artifact file payload")
				}
				if err := StageFile(typePl.GetPath(), stream); err != nil {
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

func StageFile(filename string, stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceClient) error {
	fd, err := os.Open(filename)
	if err != nil {
		return err
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

			if sendErr != nil {
				return errors.Wrap(sendErr, "chunk send failed")
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

func StageViaLegacyApi(ctx context.Context, cc *grpc.ClientConn, binary, st string) (retrievalToken string, err error) {
	client := jobpb.NewLegacyArtifactStagingServiceClient(cc)

	files := []artifact.KeyedFile{artifact.KeyedFile{Key: "worker", Filename: binary}}

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
