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
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
)

var metSpecs = (pipepb.MonitoringInfoSpecs_Enum)(0).Descriptor().Values()

// makeInfo generates dummy Monitoring infos from a spec.
func makeInfo(enum pipepb.MonitoringInfoSpecs_Enum) *pipepb.MonitoringInfo {
	spec := proto.GetExtension(metSpecs.ByNumber(protoreflect.EnumNumber(enum)).Options(), pipepb.E_MonitoringInfoSpec).(*pipepb.MonitoringInfoSpec)

	labels := map[string]string{}
	for _, l := range spec.GetRequiredLabels() {
		labels[l] = l
	}
	return &pipepb.MonitoringInfo{
		Urn:    spec.GetUrn(),
		Type:   spec.GetType(),
		Labels: labels,
	}
}

func makeInfoWBytes(enum pipepb.MonitoringInfoSpecs_Enum, payload []byte) *pipepb.MonitoringInfo {
	info := makeInfo(enum)
	info.Payload = payload
	return info
}

// This test validates that multiple contributions are correctly summed up and accumulated.
func Test_metricsStore_ContributeMetrics(t *testing.T) {

	doubleBytes := func(v float64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, math.Float64bits(v))
		return b
	}

	progress := func(vs ...float64) []byte {
		var buf bytes.Buffer
		coder.EncodeInt32(int32(len(vs)), &buf)
		for _, v := range vs {
			coder.EncodeDouble(v, &buf)
		}
		return buf.Bytes()
	}

	tests := []struct {
		name string

		// TODO convert input to non-legacy metrics once we support, and then delete these.
		input    []map[string][]byte
		shortIDs map[string]*pipepb.MonitoringInfo

		want []*pipepb.MonitoringInfo
	}{
		{
			name: "int64Sum",
			input: []map[string][]byte{
				{"a": []byte{3}},
				{"a": []byte{5}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_INT64, []byte{8}),
			},
		}, {
			name: "float64Sum",
			input: []map[string][]byte{
				{"a": doubleBytes(3.14)},
				{"a": doubleBytes(1.06)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SUM_DOUBLE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SUM_DOUBLE, doubleBytes(4.20)),
			},
		}, {
			name: "progress",
			input: []map[string][]byte{
				{"a": progress(1, 2.2, 78)},
				{"a": progress(0, 7.8, 22)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_WORK_REMAINING),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_WORK_REMAINING, progress(0, 7.8, 22)),
			},
		}, {
			name: "int64Distribution",
			input: []map[string][]byte{
				{"a": []byte{1, 2, 2, 2}},
				{"a": []byte{3, 17, 5, 7}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_DISTRIBUTION_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_DISTRIBUTION_INT64, []byte{4, 19, 2, 7}),
			},
		}, {
			name: "int64Gauge",
			input: []map[string][]byte{
				{"a": []byte{3, 5}},
				{"a": []byte{14, 2}},
				{"a": []byte{10, 18}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_LATEST_INT64),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_LATEST_INT64, []byte{14, 2}),
			},
		}, {
			name: "float64Gauge",
			input: []map[string][]byte{
				{"a": append([]byte{2}, doubleBytes(45)...)},
				{"a": append([]byte{17}, doubleBytes(2)...)},
				{"a": append([]byte{16}, doubleBytes(200)...)},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_LATEST_DOUBLE),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_LATEST_DOUBLE, append([]byte{17}, doubleBytes(2)...)),
			},
		}, {
			name: "stringSet",
			input: []map[string][]byte{
				{"a": []byte{0, 0, 0, 1, 1, 63}},
				{"a": []byte{0, 0, 0, 2, 1, 63, 1, 63}},
			},
			shortIDs: map[string]*pipepb.MonitoringInfo{
				"a": makeInfo(pipepb.MonitoringInfoSpecs_USER_SET_STRING),
			},
			want: []*pipepb.MonitoringInfo{
				makeInfoWBytes(pipepb.MonitoringInfoSpecs_USER_SET_STRING, []byte{0, 0, 0, 1, 1, 63}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ms := metricsStore{}

			ms.AddShortIDs(&fnpb.MonitoringInfosMetadataResponse{
				MonitoringInfo: test.shortIDs,
			})

			for _, payload := range test.input {
				ms.ContributeFinalMetrics(&fnpb.ProcessBundleResponse{
					MonitoringData: payload,
				})
			}

			got := ms.Results(committed)

			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("metricsStore.ContributeMetrics(%v) diff (-want,+got):\n%v", test.input, diff)
			}
		})
	}
}
