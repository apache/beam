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
	"fmt"
	"math/rand"
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
	register.DoFn2x0[string, func(int, string)]((*roundRobinKeyFn)(nil))
	register.Emitter1[beam.X]()
	register.Emitter1[string]()
	register.Emitter2[int, string]()
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

type WriteOption func(*writeConfig)

type writeConfig struct {
	suffix    string
	numShards int
}

// WithSuffix sets the file suffix (default: ".avro")
func WithSuffix(suffix string) WriteOption {
	return func(c *writeConfig) {
		c.suffix = suffix
	}
}

// WithNumShards sets the number of output shards (default: 1)
func WithNumShards(numShards int) WriteOption {
	return func(c *writeConfig) {
		c.numShards = numShards
	}
}

// Write writes a PCollection<string> to an AVRO file.
// Write expects a JSON string with a matching AVRO schema.
// the process will fail if the schema does not match the JSON
// provided
//
// Parameters:
//
//	prefix: File path prefix (e.g., "gs://bucket/output")
//	suffix: File extension (e.g., ".avro")
//	numShards: Number of output files (0 or 1 for single file)
//	schema: AVRO schema as JSON string
//
// Files are named as: <prefix>-<shard>-of-<numShards><suffix>
// Example: output-00000-of-00010.avro
//
// Examples:
//
//	Write(s, "gs://bucket/output", schema, col)                                    // output-00000-of-00001.avro (defaults)
//	Write(s, "gs://bucket/output", schema, col, WithSuffix(".avro"))               // output-00000-of-00001.avro (explicit)
//	Write(s, "gs://bucket/output", schema, col, WithNumShards(10))                 // output-00000-of-00010.avro (10 shards)
//	Write(s, "gs://bucket/output", schema, col, WithSuffix(".avro"), WithNumShards(10)) // full control
func Write(s beam.Scope, prefix, schema string, col beam.PCollection, opts ...WriteOption) {
	s = s.Scope("avroio.WriteSharded")
	filesystem.ValidateScheme(prefix)

	config := &writeConfig{
		suffix:    ".avro",
		numShards: 1,
	}

	for _, opt := range opts {
		opt(config)
	}

	// Default to single shard if not specified or 0
	if config.numShards <= 0 {
		config.numShards = 1
	}

	keyed := beam.ParDo(s, &roundRobinKeyFn{NumShards: config.numShards}, col)

	grouped := beam.GroupByKey(s, keyed)

	beam.ParDo0(s, &writeAvroFn{
		Prefix:    prefix,
		NumShards: config.numShards,
		Suffix:    config.suffix,
		Schema:    schema,
	}, grouped)
}

type roundRobinKeyFn struct {
	NumShards   int `json:"num_shards"`
	counter     int
	initialized bool
}

func (f *roundRobinKeyFn) StartBundle(emit func(int, string)) {
	f.initialized = false
}

func (f *roundRobinKeyFn) ProcessElement(element string, emit func(int, string)) {
	if !f.initialized {
		f.counter = rand.Intn(f.NumShards)
		f.initialized = true
	}
	emit(f.counter, element)
	f.counter = (f.counter + 1) % f.NumShards
}

// formatShardName creates filename: prefix-SSSSS-of-NNNNN.suffix
func formatShardName(prefix, suffix string, shardNum, numShards int) string {
	width := max(len(fmt.Sprintf("%d", numShards-1)), 5)
	return fmt.Sprintf("%s-%0*d-of-%0*d%s", prefix, width, shardNum, width, numShards, suffix)
}

type writeAvroFn struct {
	Prefix    string `json:"prefix"`
	Suffix    string `json:"suffix"`
	NumShards int    `json:"num_shards"`
	Schema    string `json:"schema"`
}

func (w *writeAvroFn) ProcessElement(ctx context.Context, shardNum int, lines func(*string) bool) (err error) {
	filename := formatShardName(w.Prefix, w.Suffix, shardNum, w.NumShards)
	log.Infof(ctx, "Writing AVRO shard %d/%d to %s", shardNum+1, w.NumShards, filename)

	fs, err := filesystem.New(ctx, filename)
	if err != nil {
		return
	}
	defer fs.Close()

	fd, err := fs.OpenWrite(ctx, filename)
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
