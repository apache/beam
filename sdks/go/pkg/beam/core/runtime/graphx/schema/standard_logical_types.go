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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
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
// since 1970-01-01 in an INT64 field. Time and String normalize out of range
// fields as time.Date does. Encoding fails unless the fields form a valid
// date with a year from -999999999 to 999999999, and decoding fails for a day
// count outside that range.
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

// minDateDays and maxDateDays bound the supported days since the epoch.
var (
	minDateDays = time.Date(-999999999, time.January, 1, 0, 0, 0, 0, time.UTC).Unix() / secondsPerDay
	maxDateDays = time.Date(999999999, time.December, 31, 0, 0, 0, 0, time.UTC).Unix() / secondsPerDay
)

func dateToStorage(d Date) (int64, error) {
	t := d.Time()
	// Out of range fields can overflow in time.Date, so only a date that
	// round trips is encoded.
	if DateOf(t) != d {
		return 0, errors.Errorf("%d-%02d-%02d is not a valid date", d.Year, d.Month, d.Day)
	}
	days := t.Unix() / secondsPerDay
	if days < minDateDays || days > maxDateDays {
		return 0, errors.Errorf("date %v is out of range", d)
	}
	return days, nil
}

func dateFromStorage(days int64) (Date, error) {
	if days < minDateDays || days > maxDateDays {
		return Date{}, errors.Errorf("%v days since the epoch is out of the supported date range", days)
	}
	return DateOf(time.Unix(days*secondsPerDay, 0).UTC()), nil
}

// MicrosInstant is a timestamp with microsecond precision, the
// beam:logical_type:micros_instant:v1 logical type. It is stored as a row of
// the seconds and the microseconds since the epoch. Encoding a value with
// sub microsecond precision fails; truncate such values with time.Time.Truncate
// first.
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
	if s.Micros < 0 || s.Micros > 999999 {
		return MicrosInstant{}, errors.Errorf("MicrosInstant micros %v is out of range [0, 999999]", s.Micros)
	}
	return MicrosInstant(time.Unix(s.Seconds, s.Micros*1000).UTC()), nil
}

// registerStandardLogicalTypes registers the standard logical types with the
// registry.
func registerStandardLogicalTypes(r *Registry) {
	registerLogicalTypeConversion(r, ToLogicalType(URNDate, typeOf[Date](), reflectx.Int64), dateToStorage, dateFromStorage)
	registerLogicalTypeConversion(r, ToLogicalType(URNMicrosInstant, typeOf[MicrosInstant](), typeOf[microsInstantStorage]()), microsInstantToStorage, microsInstantFromStorage)
}

func init() {
	// Pipeline graphs serialize the standard logical types by name.
	runtime.RegisterType(typeOf[Date]())
	runtime.RegisterType(typeOf[MicrosInstant]())
}
