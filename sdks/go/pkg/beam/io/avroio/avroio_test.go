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

package avroio

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"

	"github.com/linkedin/goavro/v2"
)

type Tweet struct {
	Stamp int64  `json:"timestamp"`
	Tweet string `json:"tweet"`
	User  string `json:"username"`
}

func TestRead(t *testing.T) {
	avroFile := "../../../../data/tweet.avro"

	p := beam.NewPipeline()
	s := p.Root()
	tweets := Read(s, avroFile, reflect.TypeOf(Tweet{}))
	passert.Count(s, tweets, "NumUsers", 1)
	passert.Equals(s, tweets, Tweet{
		Stamp: int64(20),
		Tweet: "Hello twitter",
		User:  "user1",
	})

	ptest.RunAndValidate(t, p)
}

// Google Bigquery exports nullable fields from bigquery to Avro
// (using "use_avro_logical_types=true") in the following way:
// {"name":"tweet","type":["null","string"],"default":null}
//
// The equivalent Go struct for this representation looks like the below:
//
//	type GoTypeFromGBQAvro struct {
//	   Tweet struct {
//	   	 	Value string `json:"string"`
//	   } `json:"tweet,omitempty"`
//	}
//
// For readability, using the below nested struct set-up.
type NullableFloat64 struct {
	Value float64 `json:"double"`
}

type NullableString struct {
	Value string `json:"string"`
}

type NullableTweet struct {
	Stamp NullableFloat64 `json:"timestamp,omitempty"`
	Tweet NullableString  `json:"tweet,omitempty"`
	User  NullableString  `json:"username,omitempty"`
}

// tweetwithnulls.avro contains the following values:
// Row 1: "timestamp": "20", "tweet": "Hello twitter", "username": "user1"
// Row 2: "timestamp": "21", "tweet": "Hello twitter again",
// with no / a null "username" field.
// This is generated via the "EXPORT DATA OPTIONS"
// command from Google Bigquery (with use_avro_logical_types=true)
// In Bigquery, "timestamp" is a nullable FLOAT,
// "tweet" is a nullable STRING, and "username" is a nullable STRING.
func TestReadWithNullableValues(t *testing.T) {
	avroFile := "../../../../data/tweetwithnulls.avro"

	p := beam.NewPipeline()
	s := p.Root()
	tweets := Read(s, avroFile, reflect.TypeOf(NullableTweet{}))
	passert.Count(s, tweets, "NumUsers", 2)
	passert.Equals(s, tweets, NullableTweet{
		Stamp: NullableFloat64{Value: 20},
		Tweet: NullableString{Value: "Hello twitter"},
		User:  NullableString{Value: "user1"},
	}, NullableTweet{
		Stamp: NullableFloat64{Value: 21},
		Tweet: NullableString{Value: "Hello twitter again"},
	})

	ptest.RunAndValidate(t, p)
}

type TwitterUser struct {
	User string `json:"username"`
	Info string `json:"info"`
}

const userSchema = `{
	"type": "record",
	"name": "user",
	"namespace": "twitter",
	"fields": [
		{ "name": "username", "type": "string" },
		{ "name": "info", "type": "string" }
	]
}`

func TestWrite(t *testing.T) {
	avroFile := "./user.avro"
	testUsername := "user1"
	testInfo := "userInfo"
	p, s, sequence := ptest.CreateList([]string{testUsername})
	format := beam.ParDo(s, func(username string, emit func(string)) {
		newUser := TwitterUser{
			User: username,
			Info: testInfo,
		}

		b, _ := json.Marshal(newUser)
		emit(string(b))
	}, sequence)
	Write(s, avroFile, userSchema, format)
	t.Cleanup(func() {
		os.Remove(avroFile)
	})

	ptest.RunAndValidate(t, p)

	if _, err := os.Stat(avroFile); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to write file %v", avroFile)
	}

	avroBytes, err := os.ReadFile(avroFile)
	if err != nil {
		t.Fatalf("Failed to read avro file: %v", err)
	}
	ocf, err := goavro.NewOCFReader(bytes.NewReader(avroBytes))
	if err != nil {
		t.Fatalf("Failed to make OCF Reader: %v", err)
	}
	var nativeData []any
	for ocf.Scan() {
		datum, err := ocf.Read()
		if err != nil {
			break // Read error sets OCFReader error
		}
		nativeData = append(nativeData, datum)
	}
	if err := ocf.Err(); err != nil {
		t.Fatalf("Error decoding avro data: %v", err)
	}
	if got, want := len(nativeData), 1; got != want {
		t.Fatalf("Avro data, got %v records, want %v", got, want)
	}
	if got, want := nativeData[0].(map[string]any)["username"], testUsername; got != want {
		t.Fatalf("User.User=%v, want %v", got, want)
	}
	if got, want := nativeData[0].(map[string]any)["info"], testInfo; got != want {
		t.Fatalf("User.User=%v, want %v", got, want)
	}
}
