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
	"log"

	tob "beam.apache.org/learning/tour-of-beam/backend/internal"
	pb "beam.apache.org/learning/tour-of-beam/backend/playground_api/api/v1"
)

func MakePgSaveRequest(userRequest tob.UserCodeRequest, sdk tob.Sdk, persistence_key string) pb.SaveSnippetRequest {
	filesProto := make([]*pb.SnippetFile, 0)
	for _, file := range userRequest.Files {
		filesProto = append(filesProto,
			&pb.SnippetFile{
				Name:    file.Name,
				Content: file.Content,
				IsMain:  file.IsMain,
			})
	}
	sdkIdx, ok := pb.Sdk_value[sdk.StorageID()]
	if !ok {
		log.Panicf("Playground SDK undefined for: %v", sdk)
	}
	return pb.SaveSnippetRequest{
		Sdk:             pb.Sdk(sdkIdx),
		Files:           filesProto,
		PipelineOptions: userRequest.PipelineOptions,
		PersistenceKey:  persistence_key,
	}
}
