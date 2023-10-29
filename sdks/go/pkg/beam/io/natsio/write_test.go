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
	"bytes"
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

func TestWrite(t *testing.T) {
	subject := "subject"

	tests := []struct {
		name  string
		input []any
		want  []jsMsg
	}{
		{
			name: "Write messages and deduplicate based on ID",
			input: []any{
				ProducerMessage{
					Subject: subject,
					ID:      "1",
					Data:    []byte("msg1a"),
				},
				ProducerMessage{
					Subject: subject,
					ID:      "1",
					Data:    []byte("msg1b"),
				},
				ProducerMessage{
					Subject: subject,
					ID:      "2",
					Data:    []byte("msg2"),
				},
			},
			want: []jsMsg{
				testMsg{
					subject: subject,
					headers: nats.Header{nats.MsgIdHdr: []string{"1"}},
					data:    []byte("msg1a"),
				},
				testMsg{
					subject: subject,
					headers: nats.Header{nats.MsgIdHdr: []string{"2"}},
					data:    []byte("msg2"),
				},
			},
		},
		{
			name: "Write messages without ID",
			input: []any{
				ProducerMessage{
					Subject: subject,
					Data:    []byte("msg1a"),
				},
				ProducerMessage{
					Subject: subject,
					Data:    []byte("msg1b"),
				},
				ProducerMessage{
					Subject: subject,
					Data:    []byte("msg2"),
				},
			},
			want: []jsMsg{
				testMsg{
					subject: subject,
					data:    []byte("msg1a"),
				},
				testMsg{
					subject: subject,
					data:    []byte("msg1b"),
				},
				testMsg{
					subject: subject,
					data:    []byte("msg2"),
				},
			},
		},
		{
			name: "Write message with headers",
			input: []any{
				ProducerMessage{
					Subject: subject,
					ID:      "1",
					Headers: nats.Header{"key": []string{"val"}},
					Data:    []byte("msg1"),
				},
			},
			want: []jsMsg{
				testMsg{
					subject: subject,
					headers: nats.Header{nats.MsgIdHdr: []string{"1"}, "key": []string{"val"}},
					data:    []byte("msg1"),
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv := newServer(t)
			uri := srv.ClientURL()
			conn := newConn(t, uri)
			js := newJetStream(t, conn)

			stream := "STREAM"
			subjects := []string{subject}
			createStream(t, ctx, js, stream, subjects)
			cons := createConsumer(t, ctx, js, stream, subjects)

			p, s := beam.NewPipelineWithRoot()

			col := beam.Create(s, tc.input...)
			Write(s, uri, col)

			ptest.RunAndValidate(t, p)

			got := fetchMessages(t, cons, len(tc.input)+1)

			if gotLen, wantLen := len(got), len(tc.want); gotLen != wantLen {
				t.Fatalf("Len() = %v, want %v", gotLen, wantLen)
			}

			for i := range got {
				if gotSubject, wantSubject := got[i].Subject(), tc.want[i].Subject(); gotSubject != wantSubject {
					t.Errorf("msg %d: Subject() = %v, want %v", i, gotSubject, wantSubject)
				}

				if gotHeaders, wantHeaders := got[i].Headers(), tc.want[i].Headers(); !cmp.Equal(
					gotHeaders,
					wantHeaders,
				) {
					t.Errorf("msg %d: Headers() = %v, want %v", i, gotHeaders, wantHeaders)
				}

				if gotData, wantData := got[i].Data(), tc.want[i].Data(); !bytes.Equal(
					gotData,
					wantData,
				) {
					t.Errorf("msg %d: Data() = %q, want %q", i, gotData, wantData)
				}
			}
		})
	}
}

type jsMsg interface {
	Subject() string
	Headers() nats.Header
	Data() []byte
}

type testMsg struct {
	subject string
	headers nats.Header
	data    []byte
}

func (m testMsg) Subject() string {
	return m.subject
}

func (m testMsg) Headers() nats.Header {
	return m.headers
}

func (m testMsg) Data() []byte {
	return m.data
}
