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

package coder

import (
	"io"
	"math"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// TODO(herohde) 5/16/2017: verify whether the below logic is actually the
// canonical encoding.

// EncodeEventTime encodes an EventTime as an uint64. The encoding is
// millis-since-epoch, but shifted so that the byte representation of negative
// values are lexicographically ordered before the byte representation of
// positive values.
func EncodeEventTime(t typex.EventTime, w io.Writer) error {
	return EncodeUint64((uint64)((time.Time)(t).Unix()-math.MinInt64), w)
}

// DecodeEventTime decodes an EventTime.
func DecodeEventTime(r io.Reader) (typex.EventTime, error) {
	unix, err := DecodeUint64(r)
	if err != nil {
		return typex.EventTime(time.Time{}), err
	}
	return typex.EventTime(time.Unix(0, ((int64)(unix)+math.MinInt64)<<10)), nil
}
