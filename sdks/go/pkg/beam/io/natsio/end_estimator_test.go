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

	"github.com/nats-io/nats.go"
)

func Test_endEstimator_Estimate(t *testing.T) {
	tests := []struct {
		name    string
		msgs    []*nats.Msg
		subject string
		want    int64
	}{
		{
			name: "Estimate end for published messages",
			msgs: []*nats.Msg{
				{
					Subject: "subject.1",
					Data:    []byte("msg1"),
				},
				{
					Subject: "subject.1",
					Data:    []byte("msg2"),
				},
				{
					Subject: "subject.2",
					Data:    []byte("msg3"),
				},
			},
			subject: "subject.1",
			want:    3,
		},
		{
			name:    "Estimate end for no published messages",
			subject: "subject.1",
			want:    1,
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
			publishMessages(ctx, t, js, tt.msgs)

			estimator := newEndEstimator(js, stream, tt.subject)
			if got := estimator.Estimate(); got != tt.want {
				t.Fatalf("Estimate() = %v, want %v", got, tt.want)
			}
		})
	}
}
