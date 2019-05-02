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
	"context"
	"database/sql"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"reflect"
	"strings"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*queryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writeFn)(nil)).Elem())
}

// writeSizeLimit is the maximum number of rows allowed to a write.
const writeRowLimit = 1000

// Read reads all rows from the given table. The table must have a schema
// compatible with the given type, t, and Read returns a PCollection<t>. If the
// table has more rows than t, then Read is implicitly a projection.
func Read(s beam.Scope, driver, dsn, table string, t reflect.Type) beam.PCollection {
	s = s.Scope(driver + ".Read")
	return query(s, driver, dsn, fmt.Sprintf("SELECT * from %v", table), t)
}

// Query executes a query. The output must have a schema compatible with the given
// type, t. It returns a PCollection<t>.
func Query(s beam.Scope, driver, dsn, q string, t reflect.Type) beam.PCollection {
	s = s.Scope(driver + ".Query")
	return query(s, driver, dsn, q, t)
}

func query(s beam.Scope, driver, dsn, query string, t reflect.Type) beam.PCollection {
	imp := beam.Impulse(s)
	return beam.ParDo(s, &queryFn{Driver: driver, Dsn: dsn, Query: query, Type: beam.EncodedType{T: t}}, imp, beam.TypeDefinition{Var: beam.XType, T: t})
}

type queryFn struct {
	// Project is the project
	Driver string `json:"driver"`
	// Project is the project
	Dsn string `json:"dsn"`
	// Table is the table identifier.
	Query string `json:"query"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *queryFn) ProcessElement(ctx context.Context, _ []byte, emit func(beam.X)) error {
	//TODO move DB Open and Close to Setup and Teardown methods or StartBundle and FinishBundle
	db, err := sql.Open(f.Driver, f.Dsn)
	if err != nil {
		return errors.Wrapf(err, "failed to open database: %v", f.Driver)
	}
	defer db.Close()
	statement, err := db.PrepareContext(ctx, f.Query)
	if err != nil {
		return errors.Wrapf(err, "failed to prepare query: %v", f.Query)
	}
	defer statement.Close()
	rows, err := statement.QueryContext(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to run query: %v", f.Query)
	}
	defer rows.Close()
	var mapper rowMapper
	var columns []string
	for rows.Next() {
		reflectRow := reflect.New(f.Type.T)
		row := reflectRow.Interface() // row : *T
		if mapper == nil {
			columns, err = rows.Columns()
			if err != nil {
				return err
			}
			columnsTypes, _ := rows.ColumnTypes()
			if mapper, err = newQueryMapper(columns, columnsTypes, f.Type.T); err != nil {
				return errors.WithContext(err, "creating rowValues mapper")
			}
		}
		rowValues, err := mapper(reflectRow)
		if err != nil {
			return err
		}
		err = rows.Scan(rowValues...)
		if err != nil {
			return errors.Wrapf(err, "failed to scan %v", f.Query)
		}
		if loader, ok := row.(MapLoader); ok {
			asDereferenceSlice(rowValues)
			loader.LoadMap(asMap(columns, rowValues))
		} else if loader, ok := row.(SliceLoader); ok {
			asDereferenceSlice(rowValues)
			loader.LoadSlice(rowValues)
		}
		emit(reflect.ValueOf(row).Elem().Interface()) // emit(*row)
	}
	return nil
}

// Write writes the elements of the given PCollection<T> to database, if columns left empty all table columns are used to insert into, otherwise selected
func Write(s beam.Scope, driver, dsn, table string, columns []string, col beam.PCollection) {
	t := col.Type().Type()
	s = s.Scope(driver + ".Write")
	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Driver: driver, Dsn: dsn, Table: table, Columns: columns, BatchSize: writeRowLimit, Type: beam.EncodedType{T: t}}, post)
}

// WriteWithBatchSize writes the elements of the given PCollection<T> to database with custom batch size. Batch size control number of elements in the batch INSERT statement.
func WriteWithBatchSize(s beam.Scope, batchSize int, driver, dsn, table string, columns []string, col beam.PCollection) {
	t := col.Type().Type()
	s = s.Scope(driver + ".Write")
	pre := beam.AddFixedKey(s, col)
	post := beam.GroupByKey(s, pre)
	beam.ParDo0(s, &writeFn{Driver: driver, Dsn: dsn, Table: table, Columns: columns, BatchSize: batchSize, Type: beam.EncodedType{T: t}}, post)
}

type writeFn struct {
	// Project is the project
	Driver string `json:"driver"`
	// Project is the project
	Dsn string `json:"dsn"`
	// Table is the table identifier.
	Table string `json:"table"`
	// Columns to inserts, if empty then all columns
	Columns []string `json:"columns"`
	//BatchSize size
	BatchSize int `json:"batchSize"`
	// Type is the encoded schema type.
	Type beam.EncodedType `json:"type"`
}

func (f *writeFn) ProcessElement(ctx context.Context, _ int, iter func(*beam.X) bool) error {
	//TODO move DB Open and Close to Setup and Teardown methods or StartBundle and FinishBundle
	db, err := sql.Open(f.Driver, f.Dsn)
	if err != nil {
		return errors.Wrapf(err, "failed to open database: %v", f.Driver)
	}
	defer db.Close()
	projection := "*"
	if len(f.Columns) > 0 {
		projection = strings.Join(f.Columns, ",")
	}
	dql := fmt.Sprintf("SELECT %v FROM  %v WHERE 1 = 0", projection, f.Table)
	query, err := db.Prepare(dql)
	if err != nil {
		return errors.Wrapf(err, "failed to prepare query: %v", f.Table)
	}
	defer query.Close()
	rows, err := query.Query()
	if err != nil {
		return errors.Wrapf(err, "failed to query: %v", f.Table)
	}
	columns, err := rows.Columns()
	if err != nil {
		return errors.Wrapf(err, "failed to discover column: %v", f.Table)
	}
	//TODO move to Setup methods
	mapper, err := newWriterRowMapper(columns, f.Type.T)
	if err != nil {
		return errors.WithContext(err, "creating row mapper")
	}
	writer, err := newWriter(f.BatchSize, f.Table, columns)
	if err != nil {
		return err
	}
	var val beam.X
	for iter(&val) {
		var row []interface{}
		var data map[string]interface{}
		if writer, ok := val.(Writer); ok {
			if data, err = writer.SaveData(); err == nil {
				row = make([]interface{}, len(columns))
				for i, column := range columns {
					row[i] = data[column]
				}
			}
		} else {
			row, err = mapper(reflect.ValueOf(val))
		}
		if err != nil {
			return errors.Wrapf(err, "failed to map row %T", val)
		}
		if err = writer.add(row); err != nil {
			return err
		}
		if err := writer.writeBatchIfNeeded(ctx, db); err != nil {
			return err
		}
	}

	if err := writer.writeIfNeeded(ctx, db); err != nil {
		return err
	}

	log.Infof(ctx, "written %v row(s) into %v", writer.totalCount, f.Table)
	return nil
}
