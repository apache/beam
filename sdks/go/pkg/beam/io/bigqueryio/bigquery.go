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

// Package bigqueryio provides transformations and utilities to interact with
// Google BigQuery. See also: https://cloud.google.com/bigquery/docs.
package bigqueryio

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// writeSizeLimit is the maximum number of rows allowed by BQ in a write.
const writeRowLimit = 10000

// writeSizeLimit is the maximum number of bytes allowed in BQ write.
const writeSizeLimit = 10485760

// Estimate for overall message overhead.for a write message in bytes.
const writeOverheadBytes = 1024

func init() {
	beam.RegisterType(reflect.TypeOf((*queryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)).Elem())
}

// QualifiedTableName is a fully qualified name of a bigquery table.
type QualifiedTableName struct {
	// Project is the Google Cloud project ID.
	Project string `json:"project"`
	// Dataset is the dataset ID within the project.
	Dataset string `json:"dataset"`
	// Table is the table ID within the dataset.
	Table string `json:"table"`
}

// String formats the qualified name as "<project>:<dataset>.<table>".
func (qn QualifiedTableName) String() string {
	return fmt.Sprintf("%v:%v.%v", qn.Project, qn.Dataset, qn.Table)
}

// NewQualifiedTableName parses "<project>:<dataset>.<table>" into a QualifiedTableName.
func NewQualifiedTableName(s string) (QualifiedTableName, error) {
	c := strings.LastIndex(s, ":")
	d := strings.LastIndex(s, ".")
	if c == -1 || d == -1 || d < c {
		return QualifiedTableName{}, errors.Errorf("table name missing components: %v", s)
	}

	project := s[:c]
	dataset := s[c+1 : d]
	table := s[d+1:]
	if strings.TrimSpace(project) == "" || strings.TrimSpace(dataset) == "" || strings.TrimSpace(table) == "" {
		return QualifiedTableName{}, errors.Errorf("table name has empty components: %v", s)
	}
	return QualifiedTableName{Project: project, Dataset: dataset, Table: table}, nil
}

// Read reads all rows from the given table. The table must have a schema
// compatible with the given type, t, and Read returns a PCollection<t>. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, project, table string, t reflect.Type) beam.PCollection {
	mustParseTable(table)

	s = s.Scope("bigquery.Read")

	// TODO(herohde) 7/13/2017: using * is probably too inefficient. We could infer
	// a focused query from the type.
	return query(s, project, fmt.Sprintf("SELECT * from [%v]", table), t)
}

// QueryOptions represents additional options for executing a query.
type QueryOptions struct {
	// UseStandardSQL enables BigQuery's Standard SQL dialect when executing a query.
	UseStandardSQL bool
}

// UseStandardSQL enables BigQuery's Standard SQL dialect when executing a query.
func UseStandardSQL() func(qo *QueryOptions) error {
	return func(qo *QueryOptions) error {
		qo.UseStandardSQL = true
		return nil
	}
}

// Query executes a query. The output must have a schema compatible with the given
// type, t. It returns a PCollection<t>.
func Query(s beam.Scope, project, q string, t reflect.Type, options ...func(*QueryOptions) error) beam.PCollection {
	s = s.Scope("bigquery.Query")
	return query(s, project, q, t, options...)
}

func query(s beam.Scope, project, query string, t reflect.Type, options ...func(*QueryOptions) error) beam.PCollection {
	mustInferSchema(t)

	queryOptions := QueryOptions{}
	for _, opt := range options {
		if err := opt(&queryOptions); err != nil {
			panic(err)
		}
	}

	imp := beam.Impulse(s)
	return beam.ParDo(s, &queryFn{Project: project, Query: query, Type: beam.EncodedType{T: t}, Options: queryOptions}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	// Project is the project
	Project string `json:"project"`
	// Table is the table identifier.
	Query string `json:"query"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
	// Options specifies additional query execution options.
	Options QueryOptions `json:"options"`
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	client, err := bigquery.NewClient(ctx, f.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	q := client.Query(f.Query)
	if !f.Options.UseStandardSQL {
		q.UseLegacySQL = true
	}

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	for {
		val := reflect.New(f.Type.T).Interface() // val : *T
		if err := it.Next(val); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}
	return nil
}

func mustInferSchema(t reflect.Type) bigquery.Schema {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("schema type must be struct: %v", t))
	}
	schema, err := bigquery.InferSchema(reflect.Zero(t).Interface())
	if err != nil {
		panic(errors.Wrapf(err, "invalid schema type: %v", t))
	}
	return schema
}

func mustParseTable(table string) QualifiedTableName {
	qn, err := NewQualifiedTableName(table)
	if err != nil {
		panic(err)
	}
	return qn
}

// TODO(herohde) 7/14/2017: allow CreateDispositions and WriteDispositions. The default
// is not quite what the Dataflow examples do.

// Write writes the elements of the given PCollection<T> to bigquery. T is required
// to be the schema type.
func Write(s beam.Scope, project, table string, col beam.PCollection) {
	t := col.Type().Type()
	mustInferSchema(t)
	qn := mustParseTable(table)

	s = s.Scope("bigquery.Write")

	// TODO(BEAM-3860) 3/15/2018: use side input instead of GBK.

	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Project: project, Table: qn, Type: beam.EncodedType{T: t}}, post)
}

type writeFn struct {
	// Project is the project
	Project string `json:"project"`
	// Table is the qualified table identifier.
	Table QualifiedTableName `json:"table"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

// Approximate the size of an element as it would appear in a BQ insert request.
func getInsertSize(v interface{}, schema bigquery.Schema) (int, error) {
	saver := bigquery.StructSaver{
		InsertID: strings.Repeat("0", 27),
		Struct:   v,
		Schema:   schema,
	}
	row, id, err := saver.Save()
	if err != nil {
		return 0, err
	}
	m := make(map[string]bq.JsonValue)
	for k, v := range row {
		m[k] = bq.JsonValue(v)
	}
	req := bq.TableDataInsertAllRequestRows{
		InsertId: id,
		Json:     m,
	}
	data, err := req.MarshalJSON()
	if err != nil {
		return 0, err
	}
	// Add 1 for comma separator between elements.
	return len(data) + 1, err
}

func (f *writeFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool) error {
	client, err := bigquery.NewClient(ctx, f.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	// TODO(herohde) 7/14/2017: should we create datasets? For now, "no".

	dataset := client.DatasetInProject(f.Table.Project, f.Table.Dataset)
	if _, err := dataset.Metadata(ctx); err != nil {
		return err
	}

	schema := mustInferSchema(f.Type.T)
	table := dataset.Table(f.Table.Table)
	if _, err := table.Metadata(ctx); err != nil {
		if !isNotFound(err) {
			return err
		}
		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
			return err
		}
	}

	var data []reflect.Value
	// This stores the running byte size estimate of a BQ request.
	size := writeOverheadBytes

	var val beam.X
	for iter(&val) {
		current, err := getInsertSize(val.(interface{}), schema)
		if err != nil {
			return errors.Wrapf(err, "bigquery write error")
		}
		if len(data)+1 > writeRowLimit || size+current > writeSizeLimit {
			// Write rows in batches to comply with BQ limits.
			if err := put(ctx, table, f.Type.T, data); err != nil {
				return errors.Wrapf(err, "bigquery write error [len=%d, size=%d]", len(data), size)
			}
			data = nil
			size = writeOverheadBytes
		} else {
			data = append(data, reflect.ValueOf(val.(interface{})))
			size += current
		}
	}
	if len(data) == 0 {
		return nil
	}
	if err := put(ctx, table, f.Type.T, data); err != nil {
		return errors.Wrapf(err, "bigquery write error [len=%d, size=%d]", len(data), size)
	}
	return nil
}

func put(ctx context.Context, table *bigquery.Table, t reflect.Type, data []reflect.Value) error {
	// list : []T to allow Put to infer the schema
	list := reflectx.MakeSlice(t, data...).Interface()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	return table.Uploader().Put(ctx, list)
}

func isNotFound(err error) bool {
	e, ok := err.(*googleapi.Error)
	return ok && e.Code == http.StatusNotFound
}
