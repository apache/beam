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

// Package databaseio provides transformations and utilities to interact with
// generic database database/sql API. See also: https://golang.org/pkg/database/sql/
package databaseio

import (
	"reflect"
	"testing"
	"time"
)

func Test_queryRecordMapperProvider(t *testing.T) {

	type User struct {
		ID          int
		DateOfBirth time.Time
		NameTest    string `column:"name"`
		Random      float64
	}

	mapper, err := newQueryMapper([]string{"id", "name", "random", "date_of_birth"}, nil, reflect.TypeOf(User{}))
	if err != nil {
		t.Fatalf("Expected nil, but got: %#v", err)
	}

	aUser := &User{}
	record, err := mapper(reflect.ValueOf(aUser))
	if err != nil {
		t.Fatalf("Expected nil, but got: %#v", err)
	}
	id, ok := record[0].(*int)
	if !ok {
		t.Errorf("record[0].(*int) failed, it's a %T", record[0])
	}
	*id = 10

	name, ok := record[1].(*string)
	if !ok {
		t.Errorf("record[1].(*string) failed, it's a %T", record[1])
	}
	*name = "test"

	random, ok := record[2].(*float64)
	if !ok {
		t.Errorf("record[2].(*float64) failed, it's a %T", record[2])
	}
	*random = 1.2

	dob, ok := record[3].(*time.Time)
	if !ok {
		t.Errorf("record[3].(*time.Time) failed, it's a %T", record[3])
	}
	now := time.Now()
	*dob = now

	if want := (&User{
		ID:          *id,
		DateOfBirth: *dob,
		NameTest:    *name,
		Random:      *random,
	}); !reflect.DeepEqual(aUser, want) {
		t.Errorf("got %v, want %v", aUser, want)
	}
}

func Test_writerRecordMapperProvider(t *testing.T) {
	type User struct {
		ID          int
		DateOfBirth time.Time
		NameTest    string `column:"name"`
		Random      float64
	}

	mapper, err := newWriterRowMapper([]string{"id", "name", "random", "date_of_birth"}, reflect.TypeOf(User{}))
	if err != nil {
		t.Fatalf("Expected nil, but got: %#v", err)
	}
	aUser := &User{
		ID:          2,
		NameTest:    "abc",
		Random:      1.6,
		DateOfBirth: time.Now(),
	}
	record, err := mapper(reflect.ValueOf(aUser))
	if err != nil {
		t.Fatalf("Expected nil, but got: %#v", err)
	}
	if record[0] != aUser.ID {
		t.Errorf("got %v, want %v", record[0], aUser.ID)
	}
	if record[1] != aUser.NameTest {
		t.Errorf("got %v, want %v", record[1], aUser.NameTest)
	}
	if record[2] != aUser.Random {
		t.Errorf("got %v, want %v", record[2], aUser.Random)
	}
	if record[3] != aUser.DateOfBirth {
		t.Errorf("got %v, want %v", record[3], aUser.DateOfBirth)
	}
}
