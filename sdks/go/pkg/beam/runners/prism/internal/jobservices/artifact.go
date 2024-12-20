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

package jobservices

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/protobuf/encoding/prototext"
)

func (s *Server) ReverseArtifactRetrievalService(stream jobpb.ArtifactStagingService_ReverseArtifactRetrievalServiceServer) error {
	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	job := s.jobs[in.GetStagingToken()]

	envs := job.Pipeline.GetComponents().GetEnvironments()
	for _, env := range envs {
		for _, dep := range env.GetDependencies() {
			slog.Debug("GetArtifact start",
				slog.Group("dep",
					slog.String("urn", dep.GetTypeUrn()),
					slog.String("payload", string(dep.GetTypePayload()))))
			stream.Send(jobpb.ArtifactRequestWrapper_builder{
				GetArtifact: jobpb.GetArtifactRequest_builder{
					Artifact: dep,
				}.Build(),
			}.Build())
			var buf bytes.Buffer
			for {
				in, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if in.GetIsLast() {
					slog.Debug("GetArtifact finished",
						slog.Group("dep",
							slog.String("urn", dep.GetTypeUrn()),
							slog.String("payload", string(dep.GetTypePayload()))),
						slog.Int("bytesReceived", buf.Len()),
						slog.String("rtype", fmt.Sprintf("%T", in.GetResponse())),
					)
					break
				}
				// Here's where we go through each environment's artifacts.
				// We do nothing with them.
				switch in.WhichResponse() {
				case jobpb.ArtifactResponseWrapper_GetArtifactResponse_case:
					buf.Write(in.GetGetArtifactResponse().GetData())

				case jobpb.ArtifactResponseWrapper_ResolveArtifactResponse_case:
					err := fmt.Errorf("unexpected ResolveArtifactResponse to GetArtifact: %v", in.WhichResponse())
					slog.Error("GetArtifact failure", slog.Any("error", err))
					return err
				}
			}
			if len(s.artifacts) == 0 {
				s.artifacts = map[string][]byte{}
			}
			s.artifacts[string(dep.GetTypePayload())] = buf.Bytes()
		}
	}
	return nil
}

func (s *Server) ResolveArtifacts(_ context.Context, req *jobpb.ResolveArtifactsRequest) (*jobpb.ResolveArtifactsResponse, error) {
	return jobpb.ResolveArtifactsResponse_builder{
		Replacements: req.GetArtifacts(),
	}.Build(), nil
}

func (s *Server) GetArtifact(req *jobpb.GetArtifactRequest, stream jobpb.ArtifactRetrievalService_GetArtifactServer) error {
	info := req.GetArtifact()
	buf, ok := s.artifacts[string(info.GetTypePayload())]
	if !ok {
		pt := prototext.Format(info)
		slog.Warn("unable to provide artifact to worker", "artifact_info", pt)
		return fmt.Errorf("unable to provide %v to worker", pt)
	}
	chunk := 128 * 1024 * 1024 // 128 MB
	var i int
	for i+chunk < len(buf) {
		stream.Send(jobpb.GetArtifactResponse_builder{
			Data: buf[i : i+chunk],
		}.Build())
		i += chunk
	}
	stream.Send(jobpb.GetArtifactResponse_builder{
		Data: buf[i:],
	}.Build())
	return nil
}
