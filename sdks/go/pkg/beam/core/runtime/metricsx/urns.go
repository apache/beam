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

package metricsx

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

// Urn is an enum type for representing urns of metrics and monitored states.
type Urn uint32

// TODO: Pull these from the protos.
var sUrns = [...]string{
	"beam:metric:user:sum_int64:v1",
	"beam:metric:user:sum_double:v1",
	"beam:metric:user:distribution_int64:v1",
	"beam:metric:user:distribution_double:v1",
	"beam:metric:user:latest_int64:v1",
	"beam:metric:user:latest_double:v1",
	"beam:metric:user:top_n_int64:v1",
	"beam:metric:user:top_n_double:v1",
	"beam:metric:user:bottom_n_int64:v1",
	"beam:metric:user:bottom_n_double:v1",

	"beam:metric:element_count:v1",
	"beam:metric:sampled_byte_size:v1",

	"beam:metric:pardo_execution_time:start_bundle_msecs:v1",
	"beam:metric:pardo_execution_time:process_bundle_msecs:v1",
	"beam:metric:pardo_execution_time:finish_bundle_msecs:v1",
	"beam:metric:ptransform_execution_time:total_msecs:v1",

	"beam:metric:ptransform_progress:remaining:v1",
	"beam:metric:ptransform_progress:completed:v1",
	"beam:metric:data_channel:read_index:v1",

	"TestingSentinelUrn", // Must remain last.
}

// The supported urns of metrics and monitored states.
const (
	UrnUserSumInt64 Urn = iota
	UrnUserSumFloat64
	UrnUserDistInt64
	UrnUserDistFloat64
	UrnUserLatestMsInt64
	UrnUserLatestMsFloat64
	UrnUserTopNInt64
	UrnUserTopNFloat64
	UrnUserBottomNInt64
	UrnUserBottomNFloat64

	UrnElementCount
	UrnSampledByteSize

	UrnStartBundle
	UrnProcessBundle
	UrnFinishBundle
	UrnTransformTotalTime

	UrnProgressRemaining
	UrnProgressCompleted
	UrnDataChannelReadIndex

	UrnTestSentinel // Must remain last.
)

// UrnToString returns a string representation of the urn.
func UrnToString(u Urn) string {
	return sUrns[u]
}

// UrnToType maps the urn to it's encoding type.
// This function is written to be inlinable by the compiler.
func UrnToType(u Urn) string {
	switch u {
	case UrnUserSumInt64, UrnElementCount, UrnStartBundle, UrnProcessBundle, UrnFinishBundle, UrnTransformTotalTime:
		return "beam:metrics:sum_int64:v1"
	case UrnUserSumFloat64:
		return "beam:metrics:sum_double:v1"
	case UrnUserDistInt64, UrnSampledByteSize:
		return "beam:metrics:distribution_int64:v1"
	case UrnUserDistFloat64:
		return "beam:metrics:distribution_double:v1"
	case UrnUserLatestMsInt64:
		return "beam:metrics:latest_int64:v1"
	case UrnUserLatestMsFloat64:
		return "beam:metrics:latest_double:v1"
	case UrnUserTopNInt64:
		return "beam:metrics:top_n_int64:v1"
	case UrnUserTopNFloat64:
		return "beam:metrics:top_n_double:v1"
	case UrnUserBottomNInt64:
		return "beam:metrics:bottom_n_int64:v1"
	case UrnUserBottomNFloat64:
		return "beam:metrics:bottom_n_double:v1"

	case UrnProgressRemaining, UrnProgressCompleted:
		return "beam:metrics:progress:v1"
	case UrnDataChannelReadIndex:
		return "beam:metrics:sum_int64:v1"

	// Monitoring Table isn't currently in the protos.
	// case ???:
	//	return "beam:metrics:monitoring_table:v1"

	case UrnTestSentinel:
		return "TestingSentinelType"

	default:
		panic("metric urn without specified type" + sUrns[u])
	}
}

// Int64Counter returns an encoded payload of the integer counter.
func Int64Counter(v int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Int64Latest returns an encoded payload of the latest seen integer value.
func Int64Latest(t time.Time, v int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(mtime.FromTime(t).Milliseconds(), &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Int64Distribution returns an encoded payload of the distribution of an
// integer value.
func Int64Distribution(count, sum, min, max int64) ([]byte, error) {
	var buf bytes.Buffer
	if err := coder.EncodeVarInt(count, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(sum, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(min, &buf); err != nil {
		return nil, err
	}
	if err := coder.EncodeVarInt(max, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ExecutionMsecUrn returns the Urn for the bundle state
func ExecutionMsecUrn(i int) Urn {
	switch i {
	case 0:
		return UrnStartBundle
	case 1:
		return UrnProcessBundle
	case 2:
		return UrnFinishBundle
	case 3:
		return UrnTransformTotalTime
	default:
		panic(fmt.Errorf("invalid bundle processing state: %d", i))
	}
}
