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
	"bytes"
	"encoding/gob"
	"io"
	"math/big"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// init registers the concrete types that bigquery.QueryParameter.Value may hold
// so gob can encode/decode them through the interface{} field.
func init() {
	// bigquery's paramType/paramValue only recognize
	// *bigquery.QueryParameterValue as an explicitly-typed parameter.
	gob.Register(&bigquery.QueryParameterValue{})
	gob.Register(bigquery.NullInt64{})
	gob.Register(bigquery.NullFloat64{})
	gob.Register(bigquery.NullString{})
	gob.Register(bigquery.NullBool{})
	gob.Register(bigquery.NullTimestamp{})
	gob.Register(bigquery.NullDate{})
	gob.Register(bigquery.NullTime{})
	gob.Register(bigquery.NullDateTime{})
	gob.Register(bigquery.NullGeography{})
	gob.Register(bigquery.NullJSON{})
	gob.Register(civil.Date{})
	gob.Register(civil.Time{})
	gob.Register(civil.DateTime{})
	gob.Register(time.Time{})
	gob.Register(&big.Rat{})
	gob.Register(&bigquery.IntervalValue{})
	gob.Register(&bigquery.RangeValue{})
}

func encodeQueryParameters(params []bigquery.QueryParameter) ([]byte, error) {
	if params == nil {
		return []byte{}, nil
	}
	// validate each element to tell which paramaeter is unsupported.
	for _, p := range params {
		if err := gob.NewEncoder(io.Discard).Encode([]bigquery.QueryParameter{p}); err != nil {
			return nil, errors.Errorf(
				"bigqueryio: query parameter %q has unsupported value type %T (%v). "+
					"WithQueryParameters only supports bool, string, numeric types, []byte, "+
					"time.Time, civil.Date/Time/DateTime, *big.Rat, bigquery.Null* types, "+
					"*bigquery.IntervalValue, *bigquery.RangeValue, and *bigquery.QueryParameterValue. "+
					"For STRUCT/ARRAY parameters or other custom types, build a "+
					"*bigquery.QueryParameterValue explicitly instead", p.Name, p.Value, err)
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(params); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeQueryParameters(data []byte) ([]bigquery.QueryParameter, error) {
	if len(data) == 0 {
		return []bigquery.QueryParameter{}, nil
	}
	var params []bigquery.QueryParameter
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&params); err != nil {
		return nil, err
	}
	return params, nil
}
