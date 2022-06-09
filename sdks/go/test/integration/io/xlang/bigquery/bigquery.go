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

// Package bigquery contains integration tests for cross-language BigQuery IO transforms.
package bigquery

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func init() {
	register.DoFn2x0[[]byte, func(TestRow)](&CreateTestRowsFn{})
	register.Emitter1[TestRow]()
	//register.Function1x1[TestRowPtrs, TestRow](castFn)
	beam.RegisterType(reflect.TypeOf((*TestRow)(nil)))
	beam.RegisterType(reflect.TypeOf((*RandData)(nil)))
	beam.RegisterType(reflect.TypeOf((*TestRowPtrs)(nil)))
	beam.RegisterType(reflect.TypeOf((*RandDataPtrs)(nil)))
}

const (
	// A text to shuffle to get random words.
	text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas eget nulla nec velit hendrerit placerat. Donec eu odio ultricies, fermentum arcu at, mollis lectus. Vestibulum porttitor pharetra sem vitae feugiat. Mauris facilisis neque in mauris feugiat rhoncus. Donec eu ipsum at nibh lobortis euismod. Nam at hendrerit felis. Vivamus et orci ex. Nam dui nisl, rutrum ac pretium eget, vehicula in tortor. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Phasellus ante lorem, pharetra blandit dapibus et, tempus nec purus. Maecenas in posuere sem, vel pharetra nisl. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Donec nec facilisis ex. Praesent euismod commodo efficitur. Fusce in nisi nunc."
	// Number of random elements to create for test. Must be less than number of words in text.
	inputSize = 50
)

// TestRow is a sample row to write and read from that is expected to contain enough deterministic
// and random data in different data types to provide a reasonable signal that reading and writing
// works at a basic level.
type TestRow struct {
	Counter   int64    `beam:"counter"`   // A deterministic counter, increments for each row generated.
	Rand_data RandData `beam:"rand_data"` // An inner struct containing randomized data.
}

func shuffleText() []string {
	words := strings.Fields(text)
	rand.Shuffle(len(words), func(i, j int) { words[i], words[j] = words[j], words[i] })
	return words
}

// RandData is a struct of various types of random data.
type RandData struct {
	Flip bool   `beam:"flip"` // Flip is a bool with a random chance of either result (a coin flip).
	Num  int64  `beam:"num"`  // Num is a random int64.
	Word string `beam:"word"` // Word is a randomly selected word from a sample text.
}

// ddlSchema is a string for BigQuery data definition language that corresponds to TestRow.
const ddlTestRowSchema = "counter INT64 NOT NULL, " +
	"rand_data STRUCT<" +
	"flip BOOL NOT NULL," +
	"num INT64 NOT NULL," +
	"word STRING NOT NULL" +
	"> NOT NULL"

// CreateTestRowsFn is a DoFn that creates randomized TestRows based on a seed.
type CreateTestRowsFn struct {
	seed int64
}

// ProcessElement creates a number of TestRows, populating the randomized data.
func (fn *CreateTestRowsFn) ProcessElement(_ []byte, emit func(TestRow)) {
	rand.Seed(fn.seed)
	words := shuffleText()
	for i := 0; i < inputSize; i++ {
		emit(TestRow{
			Counter: int64(i),
			Rand_data: RandData{
				Flip: rand.Int63n(2) != 0,
				Num:  rand.Int63(),
				Word: words[i],
			},
		})
	}
}

// WritePipeline creates a pipeline that writes elements created by createFn into a BigQuery table.
func WritePipeline(expansionAddr, table string, createFn interface{}) *beam.Pipeline {
	p := beam.NewPipeline()
	s := p.Root()

	// Generate elements and write to table.
	rows := beam.ParDo(s, createFn, beam.Impulse(s))
	bigqueryio.Write(s, table, rows,
		bigqueryio.CreateDisposition(bigqueryio.CreateNever),
		bigqueryio.WriteExpansionAddr(expansionAddr))

	return p
}

// ReadPipeline creates a pipeline that reads elements directly from a BigQuery table and asserts
// that they match elements created by createFn.
func ReadPipeline(expansionAddr, table string, createFn interface{}) *beam.Pipeline {
	p := beam.NewPipeline()
	s := p.Root()

	// Read from table and compare to generated elements.
	rows := beam.ParDo(s, createFn, beam.Impulse(s))
	inType := reflect.TypeOf((*TestRow)(nil)).Elem()
	readRows := bigqueryio.Read(s, inType,
		bigqueryio.FromTable(table),
		bigqueryio.ReadExpansionAddr(expansionAddr))
	passert.Equals(s, readRows, rows)

	return p
}

// TestRowPtrs is equivalent to TestRow but all fields are pointers, meant to be used when reading
// via query.
type TestRowPtrs struct {
	Counter   *int64        `beam:"counter"`
	Rand_data *RandDataPtrs `beam:"rand_data"`
}

// RandDataPtrs is equivalent to RandData but all fields are pointers, meant to be used when reading
// via query.
type RandDataPtrs struct {
	Flip *bool   `beam:"flip"`
	Num  *int64  `beam:"num"`
	Word *string `beam:"word"`
}

func castFn(elm struct {
	Counter   *int64 `beam:"counter"`
	Rand_data *struct {
		Flip *bool   `beam:"flip"`
		Num  *int64  `beam:"num"`
		Word *string `beam:"word"`
	} `beam:"rand_data"`
}) TestRow {
	return TestRow{
		Counter: *elm.Counter,
		Rand_data: RandData{
			Flip: *elm.Rand_data.Flip,
			Num:  *elm.Rand_data.Num,
			Word: *elm.Rand_data.Word,
		},
	}
}

// ReadPipeline creates a pipeline that reads elements from a BigQuery table via a SQL Query, and
// asserts that they match elements created by createFn.
func ReadFromQueryPipeline(expansionAddr, table string, createFn interface{}) *beam.Pipeline {
	p := beam.NewPipeline()
	s := p.Root()

	// Read from table and compare to generated elements.
	rows := beam.ParDo(s, createFn, beam.Impulse(s))
	inType := reflect.TypeOf((*struct {
		Counter   *int64 `beam:"counter"`
		Rand_data *struct {
			Flip *bool   `beam:"flip"`
			Num  *int64  `beam:"num"`
			Word *string `beam:"word"`
		} `beam:"rand_data"`
	})(nil)).Elem()
	query := fmt.Sprintf("SELECT * FROM `%s`", table)
	readRows := bigqueryio.Read(s, inType,
		bigqueryio.FromQuery(query),
		bigqueryio.ReadExpansionAddr(expansionAddr))
	castRows := beam.ParDo(s, castFn, readRows)
	passert.Equals(s, castRows, rows)

	return p
}
