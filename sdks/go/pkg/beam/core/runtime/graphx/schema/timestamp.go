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

package schema

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// URNTimestamp identifies the Timestamp logical type.
const URNTimestamp = "beam:logical_type:timestamp:v1"

// The beam:logical_type:timestamp:v1 logical type is a timestamp with a
// precision argument: the number of decimal digits of the subsecond field, so
// 3 for milliseconds, 6 for microseconds and 9 for nanoseconds. It is stored
// as a row of the seconds since the epoch and the non negative subseconds in
// units of 10^-precision seconds. The subseconds field is INT16 for a
// precision below 5 and INT32 otherwise.
//
// The Go SDK registers one Go type per common precision. Encoding a value
// with more digits of precision than the type fails; truncate such values with
// time.Time.Truncate first.
type (
	// TimestampMillis is beam:logical_type:timestamp:v1 with precision 3.
	TimestampMillis time.Time
	// TimestampMicros is beam:logical_type:timestamp:v1 with precision 6.
	TimestampMicros time.Time
	// TimestampNanos is beam:logical_type:timestamp:v1 with precision 9.
	TimestampNanos time.Time
)

// Time returns the timestamp as a time.Time.
func (t TimestampMillis) Time() time.Time { return time.Time(t) }

// Time returns the timestamp as a time.Time.
func (t TimestampMicros) Time() time.Time { return time.Time(t) }

// Time returns the timestamp as a time.Time.
func (t TimestampNanos) Time() time.Time { return time.Time(t) }

// timestampShortStorage is the storage type of timestamps with a precision
// below 5.
type timestampShortStorage struct {
	Seconds    int64 `beam:"seconds"`
	Subseconds int16 `beam:"subseconds"`
}

// timestampStorage is the storage type of timestamps with a precision of 5
// or more.
type timestampStorage struct {
	Seconds    int64 `beam:"seconds"`
	Subseconds int32 `beam:"subseconds"`
}

// timestampToStorage splits t into seconds and subseconds at the precision.
func timestampToStorage(t time.Time, precision int) (int64, int64, error) {
	unit := int64(1)
	for i := precision; i < 9; i++ {
		unit *= 10
	}
	nanos := int64(t.Nanosecond())
	if nanos%unit != 0 {
		return 0, 0, errors.Errorf("timestamp %v has more than %d digits of subsecond precision", t, precision)
	}
	return t.Unix(), nanos / unit, nil
}

// timestampFromStorage combines seconds and subseconds at the precision.
func timestampFromStorage(seconds, subseconds int64, precision int) (time.Time, error) {
	unit, limit := int64(1), int64(1)
	for i := 0; i < 9; i++ {
		if i < precision {
			limit *= 10
		} else {
			unit *= 10
		}
	}
	if subseconds < 0 || subseconds >= limit {
		return time.Time{}, errors.Errorf("invalid subseconds %d for timestamp with precision %d", subseconds, precision)
	}
	return time.Unix(seconds, subseconds*unit).UTC(), nil
}

func timestampMillisToStorage(t TimestampMillis) (timestampShortStorage, error) {
	seconds, subseconds, err := timestampToStorage(time.Time(t), 3)
	return timestampShortStorage{Seconds: seconds, Subseconds: int16(subseconds)}, err
}

func timestampMillisFromStorage(s timestampShortStorage) (TimestampMillis, error) {
	t, err := timestampFromStorage(s.Seconds, int64(s.Subseconds), 3)
	return TimestampMillis(t), err
}

func timestampMicrosToStorage(t TimestampMicros) (timestampStorage, error) {
	seconds, subseconds, err := timestampToStorage(time.Time(t), 6)
	return timestampStorage{Seconds: seconds, Subseconds: int32(subseconds)}, err
}

func timestampMicrosFromStorage(s timestampStorage) (TimestampMicros, error) {
	t, err := timestampFromStorage(s.Seconds, int64(s.Subseconds), 6)
	return TimestampMicros(t), err
}

func timestampNanosToStorage(t TimestampNanos) (timestampStorage, error) {
	seconds, subseconds, err := timestampToStorage(time.Time(t), 9)
	return timestampStorage{Seconds: seconds, Subseconds: int32(subseconds)}, err
}

func timestampNanosFromStorage(s timestampStorage) (TimestampNanos, error) {
	t, err := timestampFromStorage(s.Seconds, int64(s.Subseconds), 9)
	return TimestampNanos(t), err
}

// registerTimestampLogicalTypes registers the timestamp logical types with
// the registry.
func registerTimestampLogicalTypes(r *Registry) {
	registerLogicalTypeConversion(r, ToLogicalTypeWithArgument(URNTimestamp, typeOf[TimestampMillis](), typeOf[timestampShortStorage](), int32(3)), timestampMillisToStorage, timestampMillisFromStorage)
	registerLogicalTypeConversion(r, ToLogicalTypeWithArgument(URNTimestamp, typeOf[TimestampMicros](), typeOf[timestampStorage](), int32(6)), timestampMicrosToStorage, timestampMicrosFromStorage)
	registerLogicalTypeConversion(r, ToLogicalTypeWithArgument(URNTimestamp, typeOf[TimestampNanos](), typeOf[timestampStorage](), int32(9)), timestampNanosToStorage, timestampNanosFromStorage)
}
