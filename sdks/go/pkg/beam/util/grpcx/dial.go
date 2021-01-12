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

package grpcx

import (
	"context"
	"math"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"google.golang.org/grpc"
)

// Dial is a convenience wrapper over grpc.Dial. It can be overridden
// to provide a customized dialing behavior.
var Dial = DefaultDial

// DefaultDial is a dialer that specifies an insecure blocking connection with a timeout.
func DefaultDial(ctx context.Context, endpoint string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, endpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial server at %v", endpoint)
	}
	return cc, nil
}
