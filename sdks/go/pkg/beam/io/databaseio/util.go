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
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// mapFields maps column into field index in record type
func mapFields(columns []string, recordType reflect.Type) ([]int, error) {
	var indexedFields = map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		if isExported := recordType.Field(i).PkgPath == ""; !isExported {
			continue
		}
		fieldName := recordType.Field(i).Name
		indexedFields[fieldName] = i
		indexedFields[strings.ToLower(fieldName)] = i //to account for various matching strategies
		aTag := recordType.Field(i).Tag
		if column := aTag.Get("column"); column != "" {
			indexedFields[column] = i
		}
	}
	var mappedFieldIndex = make([]int, len(columns))
	for i, column := range columns {
		fieldIndex, ok := indexedFields[column]
		if !ok {
			fieldIndex, ok = indexedFields[strings.ToLower(column)]
		}
		if !ok {
			fieldIndex, ok = indexedFields[strings.Replace(strings.ToLower(column), "_", "", strings.Count(column, "_"))]
		}
		if !ok {
			return nil, errors.Errorf("failed to matched a %v field for SQL column: %v", recordType, column)
		}
		mappedFieldIndex[i] = fieldIndex
	}
	return mappedFieldIndex, nil
}

func asDereferenceSlice(aSlice []any) {
	for i, value := range aSlice {
		if value == nil {
			continue
		}
		aSlice[i] = reflect.ValueOf(value).Elem().Interface()

	}
}

func asMap(keys []string, values []any) map[string]any {
	var result = make(map[string]any)
	for i, key := range keys {
		result[key] = values[i]
	}
	return result
}
