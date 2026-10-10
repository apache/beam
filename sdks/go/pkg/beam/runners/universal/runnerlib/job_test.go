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
	"errors"
	"io"
	"testing"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"google.golang.org/grpc"
)

type msgStream struct {
	grpc.ClientStream
	msgs []*jobpb.JobMessagesResponse
}

func (s *msgStream) Recv() (*jobpb.JobMessagesResponse, error) {
	if len(s.msgs) == 0 {
		return nil, io.EOF
	}
	m := s.msgs[0]
	s.msgs = s.msgs[1:]
	return m, nil
}

type jobClient struct {
	jobpb.JobServiceClient
	stream *msgStream
}

func (c *jobClient) GetMessageStream(context.Context, *jobpb.JobMessagesRequest, ...grpc.CallOption) (jobpb.JobService_GetMessageStreamClient, error) {
	return c.stream, nil
}

func stateMsg(st jobpb.JobState_Enum) *jobpb.JobMessagesResponse {
	return &jobpb.JobMessagesResponse{
		Response: &jobpb.JobMessagesResponse_StateResponse{
			StateResponse: &jobpb.JobStateEvent{State: st},
		},
	}
}

func errMsg(text, id string) *jobpb.JobMessagesResponse {
	return &jobpb.JobMessagesResponse{
		Response: &jobpb.JobMessagesResponse_MessageResponse{
			MessageResponse: &jobpb.JobMessage{
				MessageId:   id,
				MessageText: text,
				Importance:  jobpb.JobMessage_JOB_MESSAGE_ERROR,
			},
		},
	}
}

func TestWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	const jobID = "job-1"

	t.Run("done", func(t *testing.T) {
		err := WaitForCompletion(ctx, &jobClient{stream: &msgStream{msgs: []*jobpb.JobMessagesResponse{stateMsg(jobpb.JobState_DONE)}}}, jobID)
		if err != nil {
			t.Fatalf("WaitForCompletion = %v, want nil", err)
		}
	})

	t.Run("failedWithID", func(t *testing.T) {
		err := WaitForCompletion(ctx, &jobClient{stream: &msgStream{msgs: []*jobpb.JobMessagesResponse{
			errMsg("worker gone", "beam:job:failure:timeout"),
			stateMsg(jobpb.JobState_FAILED),
		}}}, jobID)
		var je *JobError
		if !errors.As(err, &je) {
			t.Fatalf("WaitForCompletion = %v, want wrapped *JobError", err)
		}
		if je.JobID != jobID || je.Message != "worker gone" || je.MessageID != "beam:job:failure:timeout" {
			t.Fatalf("JobError = %+v", je)
		}
		want := "job job-1 failed:\nworker gone"
		if err.Error() != want {
			t.Fatalf("Error() = %q, want %q", err.Error(), want)
		}
	})

	t.Run("failedWithoutID", func(t *testing.T) {
		err := WaitForCompletion(ctx, &jobClient{stream: &msgStream{msgs: []*jobpb.JobMessagesResponse{
			stateMsg(jobpb.JobState_FAILED),
			errMsg("user panic", ""),
		}}}, jobID)
		var je *JobError
		if !errors.As(err, &je) || je.Message != "user panic" || je.MessageID != "" {
			t.Fatalf("JobError = %+v from %v", je, err)
		}
	})

	t.Run("failedNoMessage", func(t *testing.T) {
		err := WaitForCompletion(ctx, &jobClient{stream: &msgStream{msgs: []*jobpb.JobMessagesResponse{
			stateMsg(jobpb.JobState_FAILED),
		}}}, jobID)
		var je *JobError
		if !errors.As(err, &je) || je.Message != "<no error received>" || je.MessageID != "" {
			t.Fatalf("JobError = %+v from %v", je, err)
		}
	})
}
