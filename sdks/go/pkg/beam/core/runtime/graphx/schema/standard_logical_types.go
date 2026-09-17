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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// URNs of the standard logical types defined in schema.proto.
const (
	// URNDate identifies the Date logical type.
	URNDate = "beam:logical_type:date:v1"
	// URNMicrosInstant identifies the MicrosInstant logical type.
	URNMicrosInstant = "beam:logical_type:micros_instant:v1"
)

// Date is a calendar date without a time zone or time of day, the
// beam:logical_type:date:v1 logical type. It is stored as the number of days
// since 1970-01-01 in an INT64 field. Out of range fields are normalized as
// by time.Date.
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

// DateOf returns the Date of t in t's location.
func DateOf(t time.Time) Date {
	y, m, d := t.Date()
	return Date{Year: y, Month: m, Day: d}
}

// Time returns the start of the date in UTC.
func (d Date) Time() time.Time {
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC)
}

// String returns the date in ISO 8601 format, such as 2006-01-02.
func (d Date) String() string {
	return d.Time().Format("2006-01-02")
}

const secondsPerDay = 24 * 60 * 60

func dateToStorage(d Date) (int64, error) {
	return d.Time().Unix() / secondsPerDay, nil
}

func dateFromStorage(days int64) (Date, error) {
	return DateOf(time.Unix(days*secondsPerDay, 0).UTC()), nil
}

// MicrosInstant is a timestamp with microsecond precision, the
// beam:logical_type:micros_instant:v1 logical type. It is stored as a row of
// the seconds and the microseconds since the epoch. Encoding a value with
// sub microsecond precision fails, matching the Java SDK; truncate such values
// with time.Time.Truncate first.
type MicrosInstant time.Time

// Time returns the instant as a time.Time.
func (m MicrosInstant) Time() time.Time {
	return time.Time(m)
}

// microsInstantStorage is the storage type of MicrosInstant.
type microsInstantStorage struct {
	Seconds int64 `beam:"seconds"`
	Micros  int64 `beam:"micros"`
}

func microsInstantToStorage(m MicrosInstant) (microsInstantStorage, error) {
	t := time.Time(m)
	nanos := t.Nanosecond()
	if nanos%1000 != 0 {
		return microsInstantStorage{}, errors.Errorf("MicrosInstant %v has sub microsecond precision", t)
	}
	return microsInstantStorage{Seconds: t.Unix(), Micros: int64(nanos / 1000)}, nil
}

func microsInstantFromStorage(s microsInstantStorage) (MicrosInstant, error) {
	return MicrosInstant(time.Unix(s.Seconds, s.Micros*1000).UTC()), nil
}

// registerStandardLogicalTypes registers the standard logical types with the
// registry.
func registerStandardLogicalTypes(r *Registry) {
	registerLogicalTypeConversion(r, ToLogicalType(URNDate, typeOf[Date](), reflectx.Int64), dateToStorage, dateFromStorage)
	registerLogicalTypeConversion(r, ToLogicalType(URNMicrosInstant, typeOf[MicrosInstant](), typeOf[microsInstantStorage]()), microsInstantToStorage, microsInstantFromStorage)
}
