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

package natsio

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/nats-io/nats.go"
)

func TestRead(t *testing.T) {
	tests := []struct {
		name       string
		input      []*nats.Msg
		subject    string
		opts       []ReadOptionFn
		pubIndices []int
		want       []any
	}{
		{
			name: "Read messages from bounded stream with single subject",
			input: []*nats.Msg{
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"123"}},
					Data:    []byte("msg1"),
				},
				{
					Subject: "subject.2",
					Header:  nats.Header{nats.MsgIdHdr: []string{"124"}},
					Data:    []byte("msg2"),
				},
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"125"}},
					Data:    []byte("msg3"),
				},
			},
			subject: "subject.1",
			opts: []ReadOptionFn{
				ReadEndSeqNo(4),
			},
			pubIndices: []int{0, 2},
			want: []any{
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "123",
					Headers: map[string][]string{nats.MsgIdHdr: {"123"}},
					Data:    []byte("msg1"),
				},
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "125",
					Headers: map[string][]string{nats.MsgIdHdr: {"125"}},
					Data:    []byte("msg3"),
				},
			},
		},
		{
			name: "Read messages from bounded stream with wildcard subject",
			input: []*nats.Msg{
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"123"}},
					Data:    []byte("msg1"),
				},
				{
					Subject: "subject.2",
					Header:  nats.Header{nats.MsgIdHdr: []string{"124"}},
					Data:    []byte("msg2"),
				},
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"125"}},
					Data:    []byte("msg3"),
				},
			},
			subject: "subject.*",
			opts: []ReadOptionFn{
				ReadEndSeqNo(4),
			},
			pubIndices: []int{0, 1, 2},
			want: []any{
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "123",
					Headers: map[string][]string{nats.MsgIdHdr: {"123"}},
					Data:    []byte("msg1"),
				},
				ConsumerMessage{
					Subject: "subject.2",
					ID:      "124",
					Headers: map[string][]string{nats.MsgIdHdr: {"124"}},
					Data:    []byte("msg2"),
				},
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "125",
					Headers: map[string][]string{nats.MsgIdHdr: {"125"}},
					Data:    []byte("msg3"),
				},
			},
		},
		{
			name: "Read messages from bounded stream with custom fetch size",
			input: []*nats.Msg{
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"123"}},
					Data:    []byte("msg1"),
				},
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"124"}},
					Data:    []byte("msg2"),
				},
			},
			subject: "subject.1",
			opts: []ReadOptionFn{
				ReadFetchSize(1),
				ReadEndSeqNo(3),
			},
			pubIndices: []int{0, 1},
			want: []any{
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "123",
					Headers: map[string][]string{nats.MsgIdHdr: {"123"}},
					Data:    []byte("msg1"),
				},
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "124",
					Headers: map[string][]string{nats.MsgIdHdr: {"124"}},
					Data:    []byte("msg2"),
				},
			},
		},
		{
			name: "Read messages from bounded stream with custom start seq no",
			input: []*nats.Msg{
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"123"}},
					Data:    []byte("msg1"),
				},
				{
					Subject: "subject.1",
					Header:  nats.Header{nats.MsgIdHdr: []string{"124"}},
					Data:    []byte("msg2"),
				},
			},
			subject: "subject.1",
			opts: []ReadOptionFn{
				ReadStartSeqNo(2),
				ReadEndSeqNo(3),
			},
			pubIndices: []int{1},
			want: []any{
				ConsumerMessage{
					Subject: "subject.1",
					ID:      "124",
					Headers: map[string][]string{nats.MsgIdHdr: {"124"}},
					Data:    []byte("msg2"),
				},
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			srv := newServer(t)
			url := srv.ClientURL()
			conn := newConn(t, url)
			js := newJetStream(t, conn)

			stream := fmt.Sprintf("STREAM-%d", i)
			subjectFilter := "subject.*"

			createStream(ctx, t, js, stream, []string{subjectFilter})
			publishMessages(ctx, t, js, tt.input)

			cons := createConsumer(ctx, t, js, stream, []string{subjectFilter})
			pubMsgs := fetchMessages(t, cons, len(tt.input))
			wantWTime := messagesWithPublishingTime(t, pubMsgs, tt.pubIndices, tt.want)

			p, s := beam.NewPipelineWithRoot()
			got := Read(s, url, stream, tt.subject, tt.opts...)

			passert.Equals(s, got, wantWTime...)
			ptest.RunAndValidate(t, p)
		})
	}
}
