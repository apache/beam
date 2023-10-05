// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigtable

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"testing"
	"time"

	bt "cloud.google.com/go/bigtable"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigtableio"
	xlangbt "github.com/apache/beam/sdks/v2/go/pkg/beam/io/xlang/bigtableio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/dataflow"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

var expansionAddr string

func init() {
	beam.RegisterDoFn(&CreateTestRowsFn{})
	beam.RegisterDoFn(&RowToMutationFn{})

	register.DoFn2x0[[]byte, func(xlangbt.Row)](&CreateTestRowsFn{})
	register.DoFn2x0[xlangbt.Row, func(mutation bigtableio.Mutation)](&RowToMutationFn{})
	register.Emitter1[bigtableio.Mutation]()
	register.Emitter1[xlangbt.Row]()
}

type CreateTestRowsFn struct {
}

type RowToMutationFn struct {
}

func (fn *RowToMutationFn) ProcessElement(r xlangbt.Row, emit func(mutation bigtableio.Mutation)) {
	mut := bigtableio.NewMutation(string(r.Key))
	for cf, cols := range r.Column_families {
		for col, vals := range cols {
			for _, val := range vals {
				mut.Set(cf, col, bt.Timestamp(val.Timestamp_micros), val.Value)
			}
		}
	}
	emit(*mut)
}

func createTestRows() []xlangbt.Row {
	rows := make([]xlangbt.Row, 100)

	for i := 0; i < 100; i++ {
		key := strconv.FormatInt(int64(i), 10)
		row := xlangbt.Row{
			Key: []byte(key),
		}
		row.AddCell("cf1", "q1", []byte(strconv.FormatInt(int64(i)*1000, 10)), int64(i)*1000)

		rows[i] = row
	}
	return rows
}

func (fn *CreateTestRowsFn) ProcessElement(_ []byte, emit func(row xlangbt.Row)) {
	for _, r := range createTestRows() {
		emit(r)
	}
}

func checkFlags(t *testing.T) {
	if *integration.BigtableInstance == "" {
		t.Skip("No Bigtable instance provided.")
	}
}

func WritePipeline(project, instance, table string) *beam.Pipeline {
	p := beam.NewPipeline()
	s := p.Root()

	rows := beam.ParDo(s, &CreateTestRowsFn{}, beam.Impulse(s))
	muts := beam.ParDo(s, &RowToMutationFn{}, rows)
	bigtableio.Write(s, project, instance, table, muts)
	return p
}

func ReadPipeline(project, instance, table, expansionAddr string) *beam.Pipeline {
	p := beam.NewPipeline()
	s := p.Root()

	rowsFromBt := xlangbt.Read(s, project, instance, table, xlangbt.ReadExpansionAddr(expansionAddr))

	rows := beam.ParDo(s, &CreateTestRowsFn{}, beam.Impulse(s))

	passert.Equals(s, rowsFromBt, rows)
	return p
}

var instanceRegex = regexp.MustCompile("projects/(?P<project>[^/]+)/instances/(?P<instance>.+)")

func createTempTable(admin *bt.AdminClient) string {
	tableName := fmt.Sprintf("go_btio_it_temp_%v", time.Now().UnixNano())
	err := admin.CreateTableFromConf(context.Background(), &bt.TableConf{
		TableID: tableName,
		Families: map[string]bt.GCPolicy{
			"cf1": bt.NoGcPolicy(),
		},
	})
	if err != nil {
		panic(err)
	}

	return tableName
}

func deleteTempTable(admin *bt.AdminClient, table string) {
	_ = admin.DeleteTable(context.Background(), table)
}

func TestBigtableIO_BasicWriteRead(t *testing.T) {
	integration.CheckFilters(t)
	checkFlags(t)

	instancePath := *integration.BigtableInstance
	matches := instanceRegex.FindStringSubmatch(instancePath)
	project := matches[1]
	instance := matches[2]

	admin, err := bt.NewAdminClient(context.Background(), project, instance)
	if err != nil {
		panic(err)
	}

	table := createTempTable(admin)
	t.Cleanup(func() {
		deleteTempTable(admin, table)
	})

	write := WritePipeline(project, instance, table)
	ptest.RunAndValidate(t, write)

	read := ReadPipeline(project, instance, table, expansionAddr)
	ptest.RunAndValidate(t, read)
}

func TestMain(m *testing.M) {
	flag.Parse()
	beam.Init()

	services := integration.NewExpansionServices()
	defer func() { services.Shutdown() }()
	addr, err := services.GetAddr("gcpio")
	if err != nil {
		log.Printf("skipping missing expansion service: %v", err)
	} else {
		expansionAddr = addr
	}

	ptest.MainRet(m)
}
