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
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

type endEstimator struct {
	js      jetstream.JetStream
	stream  string
	subject string
}

func newEndEstimator(js jetstream.JetStream, stream string, subject string) *endEstimator {
	return &endEstimator{
		js:      js,
		stream:  stream,
		subject: subject,
	}
}

func (e *endEstimator) Estimate() int64 {
	ctx := context.Background()
	end, err := e.getEndSeqNo(ctx)
	if err != nil {
		panic(err)
	}
	return end
}

func (e *endEstimator) getEndSeqNo(ctx context.Context) (int64, error) {
	str, err := e.js.Stream(ctx, e.stream)
	if err != nil {
		return -1, fmt.Errorf("error getting stream: %v", err)
	}

	msg, err := str.GetLastMsgForSubject(ctx, e.subject)
	if err != nil {
		if isMessageNotFound(err) {
			return 1, nil
		}

		return -1, fmt.Errorf("error getting last message: %v", err)
	}

	return int64(msg.Sequence) + 1, nil
}

func isMessageNotFound(err error) bool {
	var jsErr jetstream.JetStreamError
	if errors.As(err, &jsErr) {
		apiErr := jsErr.APIError()
		if apiErr.ErrorCode == jetstream.JSErrCodeMessageNotFound {
			return true
		}
	}

	return false
}
