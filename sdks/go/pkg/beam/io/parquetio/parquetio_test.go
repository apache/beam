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

package parquetio

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestMain(m *testing.M) {
	ptest.Main(m)
}

type Student struct {
	Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
	Id      int64   `parquet:"name=id, type=INT64"`
	Weight  float32 `parquet:"name=weight, type=FLOAT"`
	Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
	Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
	Ignored int32   //without parquet tag and won't write
}

func TestRead(t *testing.T) {
	parquetFile := "../../../../data/student.parquet"
	p := beam.NewPipeline()
	s := p.Root()
	students := Read(s, parquetFile, reflect.TypeOf(Student{}))
	passert.Count(s, students, "Num records", 2)
	passert.Equals(s, students,
		Student{
			Name:    "StudentName",
			Age:     20,
			Id:      0,
			Weight:  50,
			Sex:     true,
			Day:     19089,
			Ignored: 0,
		},
		Student{
			Name:    "StudentName",
			Age:     21,
			Id:      1,
			Weight:  50.1,
			Sex:     false,
			Day:     19089,
			Ignored: 0,
		})

	ptest.RunAndValidate(t, p)
}

func TestWrite(t *testing.T) {
	var studentList = []any{
		Student{
			Name:    "StudentName",
			Age:     20,
			Id:      0,
			Weight:  50,
			Sex:     true,
			Day:     19089,
			Ignored: 0,
		},
		Student{
			Name:    "StudentName",
			Age:     21,
			Id:      1,
			Weight:  50.1,
			Sex:     false,
			Day:     19089,
			Ignored: 0,
		},
	}
	p, s, sequence := ptest.CreateList(studentList)
	parquetFile := "./write_student.parquet"
	Write(s, parquetFile, sequence)
	t.Cleanup(func() {
		os.Remove(parquetFile)
	})

	ptest.RunAndValidate(t, p)

	if _, err := os.Stat(parquetFile); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Failed to write file %v", parquetFile)
	}
	pf, err := local.NewLocalFileReader(parquetFile)
	if err != nil {
		t.Fatalf("Failed to read file %v. err: %v", parquetFile, err)
	}
	pr, err := reader.NewParquetReader(pf, new(Student), 4)
	if err != nil {
		t.Fatalf("Failed to create parquet reader %v. err: %v", parquetFile, err)
	}
	students, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		t.Fatalf("Failed to parse parquet file %v. err: %v", parquetFile, err)
	}
	if len(students) != len(studentList) {
		t.Fatalf("length mismatch. got %d, expect %d", len(students), len(studentList))
	}
	if !reflect.DeepEqual(students, studentList) {
		t.Fatalf("students differs from studentList. got %+v, expected %+v", students, studentList)
	}
}
