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
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func Test_queryRecordMapperProvider(t *testing.T) {

	type User struct {
		ID          int
		local       bool
		DateOfBirth time.Time
		NameTest    string `column:"name"`
		Random      float64
	}

	mapper, err := newQueryMapper([]string{"id", "name", "random", "date_of_birth"}, nil, reflect.TypeOf(User{}))
	if !assert.Nil(t, err) {
		return
	}

	aUser := &User{}
	record, err := mapper(reflect.ValueOf(aUser))
	if !assert.Nil(t, err) {
		return
	}
	id, ok := record[0].(*int)
	assert.True(t, ok)
	*id = 10

	name, ok := record[1].(*string)
	assert.True(t, ok)
	*name = "test"

	random, ok := record[2].(*float64)
	assert.True(t, ok)
	*random = 1.2

	dob, ok := record[3].(*time.Time)
	assert.True(t, ok)
	now := time.Now()
	*dob = now

	assert.EqualValues(t, &User{
		ID:          *id,
		DateOfBirth: *dob,
		NameTest:    *name,
		Random:      *random,
	}, aUser)
}

func Test_writerRecordMapperProvider(t *testing.T) {
	type User struct {
		ID          int
		local       bool
		DateOfBirth time.Time
		NameTest    string `column:"name"`
		Random      float64
	}

	mapper, err := newWriterRowMapper([]string{"id", "name", "random", "date_of_birth"}, reflect.TypeOf(User{}))
	if !assert.Nil(t, err) {
		return
	}
	aUser := &User{
		ID:          2,
		NameTest:    "abc",
		Random:      1.6,
		DateOfBirth: time.Now(),
	}
	record, err := mapper(reflect.ValueOf(aUser))
	if !assert.Nil(t, err) {
		return
	}
	assert.EqualValues(t, 2, record[0])
	assert.EqualValues(t, "abc", record[1])
	assert.EqualValues(t, 1.6, record[2])
	assert.EqualValues(t, aUser.DateOfBirth, record[3])

}
