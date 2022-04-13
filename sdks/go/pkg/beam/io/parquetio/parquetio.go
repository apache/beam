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
// This module contains all Go code used for Beam's SDKs. This file is placed
// in this directory in order to cover the go code required for Java and Python
// containers, as well as the entire Go SDK. Placing this file in the repository
// root is not possible because it causes conflicts with a pre-existing vendor
// directory.
package parquetio

import (
	"context"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func init() {
	beam.RegisterFunction(expandFn)
	beam.RegisterType(reflect.TypeOf((*parquetReadFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*parquetWriteFn)(nil)).Elem())
}

// Read reads a set of files and returns lines as a PCollection<elem>
// based on the internal avro schema of the file.
// A type - reflect.TypeOf( YourType{} ) -  with
// JSON tags can be defined or if you wish to return the raw JSON string,
// use - reflect.TypeOf("") -
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

func (a *parquetReadFn) ProcessElement(ctx context.Context, filename string, emit func(beam.X)) (err error) {
	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return
	}
	defer fs.Close()

	fd, err := fs.OpenRead(ctx, filename)
	if err != nil {
		return
	}
	defer fd.Close()

	data, err := ioutil.ReadAll(fd)
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

func (a *parquetWriteFn) ProcessElement(ctx context.Context, _ int, iter func(*interface{}) bool) error {
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

	val := reflect.New(a.Type.T).Interface()
	for iter(&val) {
		if err := pw.Write(val); err != nil {
			return err
		}
	}
	return pw.WriteStop()
}
