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

package service

import (
	context "context"

	pb "beam.apache.org/learning/tour-of-beam/backend/playground_api/api/v1"
	grpc "google.golang.org/grpc"
)

// Mock GRPC client which is enabled by TOB_MOCK env flag
func GetMockClient() pb.PlaygroundServiceClient {

	return &pb.PlaygroundServiceClientMock{
		SaveSnippetFunc: func(ctx context.Context, in *pb.SaveSnippetRequest, opts ...grpc.CallOption) (*pb.SaveSnippetResponse, error) {
			return &pb.SaveSnippetResponse{Id: "snippet_id_1"}, nil
		},
		GetSnippetFunc: func(ctx context.Context, in *pb.GetSnippetRequest, opts ...grpc.CallOption) (*pb.GetSnippetResponse, error) {
			return &pb.GetSnippetResponse{
				Files: []*pb.SnippetFile{
					{Name: "main.py", Content: "import sys; sys.exit(0)", IsMain: true},
				},
				Sdk:             pb.Sdk_SDK_PYTHON,
				PipelineOptions: "some opts",
			}, nil
		},
	}
}
