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

// Package parquetio contains transforms for reading and writing parquet files
package parquetio

import (
	"context"
	"io"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func init() {
	register.Function3x1(expandFn)
	register.Emitter1[string]()

	beam.RegisterType(reflect.TypeOf((*parquetReadFn)(nil)).Elem())
	register.DoFn3x1[context.Context, string, func(beam.X), error](&parquetReadFn{})
	register.Emitter1[beam.X]()

	beam.RegisterType(reflect.TypeOf((*parquetWriteFn)(nil)).Elem())
	register.DoFn3x1[context.Context, int, func(*beam.X) bool, error](&parquetWriteFn{})
	register.Iter1[beam.X]()
}

// Read reads a set of files and returns lines as a PCollection<elem>
// based on type of a parquetStruct (struct with parquet tags).
// For example:
//
//	type Student struct {
//	  Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
//	  Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
//	  Id      int64   `parquet:"name=id, type=INT64"`
//	  Weight  float32 `parquet:"name=weight, type=FLOAT"`
//	  Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
//	  Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
//	  Ignored int32   //without parquet tag and won't write
//	}
func Read(s beam.Scope, glob string, t reflect.Type) beam.PCollection {
	s = s.Scope("parquetio.Read")
	filesystem.ValidateScheme(glob)
	return read(s, t, beam.Create(s, glob))
}

func read(s beam.Scope, t reflect.Type, col beam.PCollection) beam.PCollection {
	files := beam.ParDo(s, expandFn, col)
	return beam.ParDo(s,
		&parquetReadFn{Type: beam.EncodedType{T: t}},
		files,
		beam.TypeDefinition{Var: beam.XType, T: t},
	)
}

func expandFn(ctx context.Context, glob string, emit func(string)) error {
	if strings.TrimSpace(glob) == "" {
		return nil // ignore empty string elements here
	}

	fs, err := filesystem.New(ctx, glob)
	if err != nil {
		return err
	}
	defer fs.Close()

	files, err := fs.List(ctx, glob)
	if err != nil {
		return err
	}
	for _, filename := range files {
		emit(filename)
	}
	return nil
}

type parquetReadFn struct {
	Type beam.EncodedType
}

func (a *parquetReadFn) ProcessElement(ctx context.Context, filename string, emit func(beam.X)) error {
	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return err
	}
	defer fd.Close()

	data, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	bufferReader := buffer.NewBufferFileFromBytes(data)
	parquetReader, err := reader.NewParquetReader(bufferReader, reflect.New(a.Type.T).Interface(), 4)
	if err != nil {
		return err
	}

	vals, err := parquetReader.ReadByNumber(int(parquetReader.GetNumRows()))
	if err != nil {
		return err
	}
	for _, v := range vals {
		emit(v)
	}

	return nil
}

// Write writes a PCollection<parquetStruct> to .parquet file.
// Write expects a type t of struct with parquet tags
// For example:
//
//	type Student struct {
//	  Name    string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
//	  Age     int32   `parquet:"name=age, type=INT32, encoding=PLAIN"`
//	  Id      int64   `parquet:"name=id, type=INT64"`
//	  Weight  float32 `parquet:"name=weight, type=FLOAT"`
//	  Sex     bool    `parquet:"name=sex, type=BOOLEAN"`
//	  Day     int32   `parquet:"name=day, type=INT32, convertedtype=DATE"`
//	  Ignored int32   //without parquet tag and won't write
//	}
func Write(s beam.Scope, filename string, t reflect.Type, col beam.PCollection) {
	s = s.Scope("parquetio.Write")
	filesystem.ValidateScheme(filename)
	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &parquetWriteFn{Filename: filename, Type: beam.EncodedType{T: t}}, post)
}

type parquetWriteFn struct {
	Type     beam.EncodedType
	Filename string `json:"filename"`
}

func (a *parquetWriteFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool) error {
	fs, err := filesystem.New(ctx, a.Filename)
	if err != nil {
		return err
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, a.Filename)
	if err != nil {
		return err
	}

	defer fd.Close()
	pw, err := writer.NewParquetWriterFromWriter(fd, reflect.New(a.Type.T).Interface(), 4)
	if err != nil {
		return err
	}

	var val beam.X
	for iter(&val) {
		if err := pw.Write(val); err != nil {
			return err
		}
	}
	return pw.WriteStop()
}
