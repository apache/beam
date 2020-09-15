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
	"database/sql"
	"fmt"
	"golang.org/x/net/context"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
)

// Writer returns a row of data to be inserted into a table.
type Writer interface {
	SaveData() (map[string]interface{}, error)
}

type writer struct {
	batchSize    int
	table        string
	sqlTemplate  string
	valueTempate string
	binding      []interface{}
	columnCount  int
	rowCount     int
	totalCount   int
}

func (w *writer) add(row []interface{}) error {
	w.rowCount++
	w.totalCount++
	if len(row) != w.columnCount {
		return errors.Errorf("expected %v row values, but had: %v", w.columnCount, len(row))
	}
	w.binding = append(w.binding, row...)
	return nil
}

func (w *writer) write(ctx context.Context, db *sql.DB) error {
	values := strings.Repeat(w.valueTempate+",", w.rowCount)
	if len(values) == 0 {
		log.Info(ctx, "No value(s) to be written....")
		return nil
	}
	SQL := w.sqlTemplate + string(values[:len(values)-1])
	resultSet, err := db.ExecContext(ctx, SQL, w.binding...)
	if err != nil {
		return err
	}
	affected, _ := resultSet.RowsAffected()
	if int(affected) != w.rowCount {
		return errors.Errorf("expected to write: %v, but written: %v", w.rowCount, affected)
	}
	w.binding = []interface{}{}
	w.rowCount = 0
	return nil
}

func (w *writer) writeBatchIfNeeded(ctx context.Context, db *sql.DB) error {
	if w.rowCount >= w.batchSize {
		return w.write(ctx, db)
	}
	return nil
}

func (w *writer) writeIfNeeded(ctx context.Context, db *sql.DB) error {
	if w.rowCount >= 0 {
		return w.write(ctx, db)
	}
	return nil
}

func newWriter(batchSize int, table string, columns []string) (*writer, error) {
	if len(columns) == 0 {
		return nil, errors.New("columns were empty")
	}
	values := strings.Repeat("?,", len(columns))
	return &writer{
		batchSize:    batchSize,
		columnCount:  len(columns),
		table:        table,
		binding:      make([]interface{}, 0),
		sqlTemplate:  fmt.Sprintf("INSERT INTO %v(%v) VALUES", table, strings.Join(columns, ",")),
		valueTempate: fmt.Sprintf("(%s)", values[:len(values)-1]),
	}, nil
}
