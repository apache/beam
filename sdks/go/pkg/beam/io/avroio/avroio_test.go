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
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

type Tweet struct {
	Stamp int64  `json:"timestamp"`
	Tweet string `json:"tweet"`
	User  string `json:"username"`
}

const schema = `{
	"type": "record",
	"name": "tweet",
	"namespace": "twitter",
	"fields": [
		{ "name": "timestamp", "type": "double" },
		{ "name": "tweet", "type": "string" },
		{ "name": "username", "type": "string" }
	]
}`

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

type User struct {
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
	p, s, sequence := ptest.CreateList([]string{testUsername})
	format := beam.ParDo(s, func(username string, emit func(string)) {
		newUser := User{
			User: username,
			Info: "Human",
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

	p = beam.NewPipeline()
	s = p.Root()
	users := Read(s, avroFile, reflect.TypeOf(User{}))
	passert.Count(s, users, "NumUsers", 1)
	passert.Equals(s, users, User{
		User: testUsername,
		Info: "Human",
	})

	ptest.RunAndValidate(t, p)
}
