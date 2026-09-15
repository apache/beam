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

package bigqueryio

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/google/go-cmp/cmp"
)

// bigRatComparer lets cmp.Diff compare *big.Rat values.
var bigRatComparer = cmp.Comparer(func(x, y *big.Rat) bool {
	if x == nil || y == nil {
		return x == y
	}
	return x.Cmp(y) == 0
})

func Test_encodeDecodeQueryOptions(t *testing.T) {
	tests := []struct {
		name string
		val  bigquery.QueryParameter
	}{
		{
			name: "string",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: "value",
			},
		},
		{
			name: "nil",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: nil,
			},
		},
		{
			name: "int",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: 50,
			},
		},
		{
			name: "int8",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: int8(50),
			},
		},
		{
			name: "int16",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: int16(50),
			},
		},
		{
			name: "int32",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: int32(50),
			},
		},
		{
			name: "int64",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: int64(50),
			},
		},
		{
			name: "uint8",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: uint8(50),
			},
		},
		{
			name: "uint16",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: uint16(50),
			},
		},
		{
			name: "uint32",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: uint32(50),
			},
		},
		{
			name: "float32",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: float32(50.5),
			},
		},
		{
			name: "float64",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: 50.0,
			},
		},
		{
			name: "bool",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: true,
			},
		},
		{
			name: "[]byte",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: []byte("value"),
			},
		},
		{
			name: "[]int slice",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: []int{1, 2, 3},
			},
		},
		{
			name: "[]string slice",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: []string{"a", "b", "c"},
			},
		},
		{
			name: "time.Time",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			},
		},
		{
			name: "civil.Date",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: civil.Date{Year: 2024, Month: 1, Day: 2},
			},
		},
		{
			name: "civil.Time",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: civil.Time{Hour: 3, Minute: 4, Second: 5},
			},
		},
		{
			name: "civil.DateTime",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: civil.DateTime{
					Date: civil.Date{Year: 2024, Month: 1, Day: 2},
					Time: civil.Time{Hour: 3, Minute: 4, Second: 5},
				},
			},
		},
		{
			name: "*big.Rat",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: big.NewRat(22, 7),
			},
		},
		{
			name: "*bigquery.IntervalValue",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: &bigquery.IntervalValue{
					Years:          1,
					Months:         2,
					Days:           3,
					Hours:          4,
					Minutes:        5,
					Seconds:        6,
					SubSecondNanos: 7,
				},
			},
		},
		{
			name: "*bigquery.RangeValue",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: &bigquery.RangeValue{
					Start: civil.Date{Year: 2024, Month: 1, Day: 1},
					End:   civil.Date{Year: 2024, Month: 12, Day: 31},
				},
			},
		},
		{
			name: "NullInt64 valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullInt64{Int64: 50, Valid: true},
			},
		},
		{
			name: "NullInt64 null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullInt64{Valid: false},
			},
		},
		{
			name: "NullFloat64 valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullFloat64{Float64: 50.5, Valid: true},
			},
		},
		{
			name: "NullFloat64 null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullFloat64{Valid: false},
			},
		},
		{
			name: "NullString valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullString{StringVal: "value", Valid: true},
			},
		},
		{
			name: "NullString null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullString{Valid: false},
			},
		},
		{
			name: "NullBool valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullBool{Bool: true, Valid: true},
			},
		},
		{
			name: "NullBool null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullBool{Valid: false},
			},
		},
		{
			name: "NullTimestamp valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullTimestamp{Timestamp: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), Valid: true},
			},
		},
		{
			name: "NullTimestamp null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullTimestamp{Valid: false},
			},
		},
		{
			name: "NullDate valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullDate{Date: civil.Date{Year: 2024, Month: 1, Day: 2}, Valid: true},
			},
		},
		{
			name: "NullDate null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullDate{Valid: false},
			},
		},
		{
			name: "NullTime valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullTime{Time: civil.Time{Hour: 3, Minute: 4, Second: 5}, Valid: true},
			},
		},
		{
			name: "NullTime null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullTime{Valid: false},
			},
		},
		{
			name: "NullDateTime valid",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: bigquery.NullDateTime{
					DateTime: civil.DateTime{
						Date: civil.Date{Year: 2024, Month: 1, Day: 2},
						Time: civil.Time{Hour: 3, Minute: 4, Second: 5},
					},
					Valid: true,
				},
			},
		},
		{
			name: "NullDateTime null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullDateTime{Valid: false},
			},
		},
		{
			name: "NullGeography valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullGeography{GeographyVal: "POINT(1 2)", Valid: true},
			},
		},
		{
			name: "NullGeography null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullGeography{Valid: false},
			},
		},
		{
			name: "NullJSON valid",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullJSON{JSONVal: `{"a":1}`, Valid: true},
			},
		},
		{
			name: "NullJSON null",
			val: bigquery.QueryParameter{
				Name:  "key",
				Value: bigquery.NullJSON{Valid: false},
			},
		},
		{
			name: "QueryParameterValue BIGNUMERIC",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: &bigquery.QueryParameterValue{
					Type: bigquery.StandardSQLDataType{
						TypeKind: "BIGNUMERIC",
					},
					Value: "12345678901234567890123456789012345678901234567890.12345678901234567890123456789012345678901234567890",
				},
			},
		},
		{
			name: "QueryParameterValue ARRAY of STRUCT",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: &bigquery.QueryParameterValue{
					Type: bigquery.StandardSQLDataType{
						ArrayElementType: &bigquery.StandardSQLDataType{
							StructType: &bigquery.StandardSQLStructType{
								Fields: []*bigquery.StandardSQLField{
									{
										Name: "NumberField",
										Type: &bigquery.StandardSQLDataType{
											TypeKind: "INT64",
										},
									},
								},
							},
						},
					},
					ArrayValue: []bigquery.QueryParameterValue{
						{StructValue: map[string]bigquery.QueryParameterValue{
							"NumberField": {
								Value: int64(42),
							},
						}},
						{StructValue: map[string]bigquery.QueryParameterValue{
							"NumberField": {
								Value: int64(43),
							},
						}},
					},
				},
			},
		},
		{
			name: "QueryParameterValue STRUCT",
			val: bigquery.QueryParameter{
				Name: "key",
				Value: &bigquery.QueryParameterValue{
					Type: bigquery.StandardSQLDataType{
						StructType: &bigquery.StandardSQLStructType{
							Fields: []*bigquery.StandardSQLField{
								{
									Name: "NumberField",
									Type: &bigquery.StandardSQLDataType{
										TypeKind: "INT64",
									},
								},
							},
						},
					},
					StructValue: map[string]bigquery.QueryParameterValue{
						"NumberField": {
							Value: int64(42),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeQueryParameters([]bigquery.QueryParameter{tt.val})
			if err != nil {
				t.Fatalf("encodeQueryParameters() error = %v", err)
			}

			decoded, err := decodeQueryParameters(encoded)
			if err != nil {
				t.Fatalf("decodeQueryParameters() error = %v", err)
			}

			gotType := reflect.TypeOf(decoded[0].Value)
			wantType := reflect.TypeOf(tt.val.Value)
			if gotType != wantType {
				t.Errorf("type not preserved: want %v, got %v", wantType, gotType)
			}

			if diff := cmp.Diff(tt.val, decoded[0], bigRatComparer); diff != "" {
				t.Errorf("encode/decode mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// customParam is a struct type deliberately not registered with gob.
type customParam struct {
	Foo string
	Bar int64
}

func TestEncodeQueryParameters_UnsupportedType(t *testing.T) {
	t.Parallel()
	_, err := encodeQueryParameters([]bigquery.QueryParameter{
		{Name: "custom", Value: customParam{Foo: "a", Bar: 1}},
	})
	if err == nil {
		t.Fatal("encodeQueryParameters() error = nil, want error for unsupported type")
	}
	if !strings.Contains(err.Error(), `"custom"`) {
		t.Errorf("encodeQueryParameters() error = %q, want it to name the parameter %q", err.Error(), "custom")
	}
	if !strings.Contains(err.Error(), "bigqueryio.customParam") {
		t.Errorf("encodeQueryParameters() error = %q, want it to name the type %q", err.Error(), "bigqueryio.customParam")
	}
}

func TestEncodeQueryParameters_UnsupportedTypeAmongSupported(t *testing.T) {
	t.Parallel()
	_, err := encodeQueryParameters([]bigquery.QueryParameter{
		{Name: "ok", Value: "value1"},
		{Name: "bad", Value: customParam{Foo: "a", Bar: 1}},
	})
	if err == nil {
		t.Fatal("encodeQueryParameters() error = nil, want error for unsupported type")
	}
	if !strings.Contains(err.Error(), `"bad"`) {
		t.Errorf("encodeQueryParameters() error = %q, want it to name the offending parameter %q, not the supported one", err.Error(), "bad")
	}
}
