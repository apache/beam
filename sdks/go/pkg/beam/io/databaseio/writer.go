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
	"strings"

	"golang.org/x/net/context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
)

// Writer returns a row of data to be inserted into a table.
type Writer interface {
	SaveData() (map[string]any, error)
}

type writer struct {
	batchSize              int
	table                  string
	sqlTemplate            string
	valueTemplateGenerator *valueTemplateGenerator
	binding                []any
	columnCount            int
	rowCount               int
	totalCount             int
}

func (w *writer) add(row []any) error {
	w.rowCount++
	w.totalCount++
	if len(row) != w.columnCount {
		return errors.Errorf("expected %v row values, but had: %v", w.columnCount, len(row))
	}
	w.binding = append(w.binding, row...)
	return nil
}

func (w *writer) write(ctx context.Context, db *sql.DB) error {
	values := w.valueTemplateGenerator.generate(w.rowCount, w.columnCount)
	if len(values) == 0 {
		log.Info(ctx, "No value(s) to be written....")
		return nil
	}
	SQL := w.sqlTemplate + values
	resultSet, err := db.ExecContext(ctx, SQL, w.binding...)
	if err != nil {
		return err
	}
	affected, _ := resultSet.RowsAffected()
	if int(affected) != w.rowCount {
		return errors.Errorf("expected to write: %v, but written: %v", w.rowCount, affected)
	}
	w.binding = []any{}
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

func newWriter(driver string, batchSize int, table string, columns []string) (*writer, error) {
	if len(columns) == 0 {
		return nil, errors.New("columns were empty")
	}
	return &writer{
		batchSize:              batchSize,
		columnCount:            len(columns),
		table:                  table,
		binding:                make([]any, 0),
		sqlTemplate:            fmt.Sprintf("INSERT INTO %v(%v) VALUES", table, strings.Join(columns, ",")),
		valueTemplateGenerator: &valueTemplateGenerator{driver},
	}, nil
}

type valueTemplateGenerator struct {
	driver string
}

func (v *valueTemplateGenerator) generate(rowCount int, columnColunt int) string {
	switch v.driver {
	case "postgres", "pgx":
		// the point is to generate ($1,$2),($3,$4)
		valueTemplates := make([]string, rowCount)
		for i := 0; i < rowCount; i++ {
			n := columnColunt * i
			templates := make([]string, columnColunt)
			for j := 0; j < columnColunt; j++ {
				templates[j] = fmt.Sprintf("$%d", n+j+1)
			}
			valueTemplates[i] = fmt.Sprintf("(%s)", strings.Join(templates, ","))
		}
		return strings.Join(valueTemplates, ",")
	default:
		// the point is to generate (?,?),(?,?)
		questions := strings.Repeat("?,", columnColunt)
		valueTemplate := fmt.Sprintf("(%s)", questions[:len(questions)-1])
		values := strings.Repeat(valueTemplate+",", rowCount)
		if len(values) == 0 {
			return values
		}
		return values[:len(values)-1]
	}
}
