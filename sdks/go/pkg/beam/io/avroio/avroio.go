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

// Package avroio contains transforms for reading and writing avro files.
package avroio

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/fileio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/linkedin/goavro/v2"
)

func init() {
	register.DoFn3x1[context.Context, fileio.ReadableFile, func(beam.X), error]((*avroReadFn)(nil))
	register.DoFn3x1[context.Context, int, func(*string) bool, error]((*writeAvroFn)(nil))
	register.Emitter1[beam.X]()
	register.Iter1[string]()
}

// Read reads a set of files and returns lines as a PCollection<elem>
// based on the internal avro schema of the file.
// A type - reflect.TypeOf( YourType{} ) -  with
// JSON tags can be defined or if you wish to return the raw JSON string,
// use - reflect.TypeOf("") -
func Read(s beam.Scope, glob string, t reflect.Type) beam.PCollection {
	s = s.Scope("avroio.Read")
	filesystem.ValidateScheme(glob)
	return read(s, t, beam.Create(s, glob))
}

func read(s beam.Scope, t reflect.Type, col beam.PCollection) beam.PCollection {
	matches := fileio.MatchAll(s, col, fileio.MatchEmptyAllow())
	files := fileio.ReadMatches(s, matches, fileio.ReadUncompressed())
	return beam.ParDo(s,
		&avroReadFn{Type: beam.EncodedType{T: t}},
		files,
		beam.TypeDefinition{Var: beam.XType, T: t},
	)
}

type avroReadFn struct {
	// Avro schema type
	Type beam.EncodedType
}

func (f *avroReadFn) ProcessElement(ctx context.Context, file fileio.ReadableFile, emit func(beam.X)) (err error) {
	log.Infof(ctx, "Reading AVRO from %v", file.Metadata.Path)

	fd, err := file.Open(ctx)
	if err != nil {
		return
	}
	defer fd.Close()

	ar, err := goavro.NewOCFReader(fd)
	if err != nil {
		log.Errorf(ctx, "error reading avro: %v", err)
		return
	}

	for ar.Scan() {
		val := reflect.New(f.Type.T).Interface()
		var i any
		i, err = ar.Read()
		if err != nil {
			log.Errorf(ctx, "error reading avro row: %v", err)
			continue
		}

		// marshal interface to bytes
		var b []byte
		b, err = json.Marshal(i)
		if err != nil {
			log.Errorf(ctx, "error unmarshalling avro data: %v", err)
			return
		}

		switch reflect.New(f.Type.T).Interface().(type) {
		case *string:
			emit(string(b))
		default:
			if err = json.Unmarshal(b, val); err != nil {
				log.Errorf(ctx, "error unmashalling avro to type: %v", err)
				return
			}
			emit(reflect.ValueOf(val).Elem().Interface())
		}
	}

	return ar.Err()
}

// Write writes a PCollection<string> to an AVRO file.
// Write expects a JSON string with a matching AVRO schema.
// the process will fail if the schema does not match the JSON
// provided
func Write(s beam.Scope, filename, schema string, col beam.PCollection) {
	s = s.Scope("avroio.Write")
	filesystem.ValidateScheme(filename)
	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeAvroFn{Schema: schema, Filename: filename}, post)
}

type writeAvroFn struct {
	Schema   string `json:"schema"`
	Filename string `json:"filename"`
}

func (w *writeAvroFn) ProcessElement(ctx context.Context, _ int, lines func(*string) bool) (err error) {
	log.Infof(ctx, "writing AVRO to %s", w.Filename)
	fs, err := filesystem.New(ctx, w.Filename)
	if err != nil {
		return
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, w.Filename)
	if err != nil {
		return
	}

	defer fd.Close()

	codec, err := goavro.NewCodec(w.Schema)
	if err != nil {
		log.Errorf(ctx, "error creating avro codec: %v", err)
		return
	}

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		Codec:           codec,
		CompressionName: goavro.CompressionSnappyLabel,
		Schema:          w.Schema,
		W:               fd,
	})

	if err != nil {
		log.Errorf(ctx, "error creating avro writer: %v", err)
		return
	}

	var j string
	for lines(&j) {
		native, _, err := codec.NativeFromTextual([]byte(j))
		if err != nil {
			log.Errorf(ctx, "error reading native avro: %v", err)
			return err
		}

		if err := ocfw.Append([]any{native}); err != nil {
			log.Errorf(ctx, "error writing avro: %v", err)
			return err
		}
	}

	return
}
