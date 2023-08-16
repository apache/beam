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

package databaseio

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	_ "github.com/proullon/ramsql/driver"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

type Address struct {
	Street        string
	Street_number int
}

func TestRead(t *testing.T) {
	db, err := sql.Open("ramsql", "user:password@/dbname")
	if err != nil {
		t.Fatalf("Test infra failure: Failed to open database with error %v", err)
	}
	defer db.Close()
	if err = insertTestData(db); err != nil {
		t.Fatalf("Test infra failure: Failed to create/populate table with error %v", err)
	}

	p, s := beam.NewPipelineWithRoot()
	elements := Read(s, "ramsql", "user:password@/dbname", "address", reflect.TypeOf(Address{}))
	passert.Count(s, elements, "NumElements", 2)
	passert.Equals(s, elements, Address{Street: "orchard lane", Street_number: 1}, Address{Street: "morris st", Street_number: 200})

	ptest.RunAndValidate(t, p)
}

func TestQuery(t *testing.T) {
	db, err := sql.Open("ramsql", "user:password@/dbname2")
	if err != nil {
		t.Fatalf("Test infra failure: Failed to open database with error %v", err)
	}
	defer db.Close()
	if err = insertTestData(db); err != nil {
		t.Fatalf("Test infra failure: Failed to create/populate table with error %v", err)
	}

	p, s := beam.NewPipelineWithRoot()
	read := Query(s, "ramsql", "user:password@/dbname2", "SELECT * FROM address WHERE street_number < 10", reflect.TypeOf(Address{}))

	passert.Count(s, read, "NumElementsFromRead", 1)
	passert.Equals(s, read, Address{Street: "orchard lane", Street_number: 1})

	ptest.RunAndValidate(t, p)
}

func insertTestData(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE address (street TEXT, street_number INT);")
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO address (street, street_number) VALUES ('orchard lane', 1);")
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO address (street, street_number) VALUES ('morris st', 200);")
	if err != nil {
		return err
	}
	return nil
}
