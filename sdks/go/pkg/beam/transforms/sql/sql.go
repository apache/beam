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

// Package sql contains SQL transform APIs, allowing SQL queries to be used
// in Beam Go pipelines.
//
// NOTE: This feature only works when an expansion service/handler is
// registered for SQL transform. The APIs are subject to change without
// backward compatibility guarantees.
package sql

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/sql/sqlx"
)

// Option is the base type of all the SQL transform options.
type Option func(sqlx.Options)

// options contain all the options for a SQL transform.
type options struct {
	dialect       string
	expansionAddr string
	inputs        map[string]beam.PCollection
	outType       beam.FullType
	customs       []sqlx.Option
}

func (o *options) Add(opt sqlx.Option) {
	o.customs = append(o.customs, opt)
}

// Input adds a named PCollection input to the transform.
func Input(name string, in beam.PCollection) Option {
	return func(o sqlx.Options) {
		o.(*options).inputs[name] = in
	}
}

// OutputType specifies the output PCollection type of the transform.
// It must match the SQL output schema.
//
// There is currently no default output type, so users must set this option.
// In the future, Row, once implemented, may become the default output type.
func OutputType(t reflect.Type, components ...typex.FullType) Option {
	return func(o sqlx.Options) {
		o.(*options).outType = typex.New(t, components...)
	}
}

// Dialect specifies the SQL dialect, e.g. use 'zetasql' for ZetaSQL.
func Dialect(dialect string) Option {
	return func(o sqlx.Options) {
		o.(*options).dialect = dialect
	}
}

// ExpansionAddr is the URL of the expansion service to use.
func ExpansionAddr(addr string) Option {
	return func(o sqlx.Options) {
		o.(*options).expansionAddr = addr
	}
}

// Transform creates a SQL-based transform over zero or more PCollections
// and/or named data sources.
//
// PCollection inputs can be added using the sql.Input option. SQL queries can
// also refer to external tables that can be resolved by the expansion service.
//
// The output PCollection type must be specified by the sql.OutputType option.
//
// Example:
//
//	in := beam.Create(s, 1, 2, 3)
//	out := sql.Transform(s, "SELECT COUNT(*) FROM t",
//	    sql.Input("t", in),
//	    sql.OutputType(reflect.TypeOf(int64(0))))
//	// `out` is a PCollection<int64> with a single element 3.
//
// If an expansion service address is not provided as an option, one will be
// automatically started for the transform.
func Transform(s beam.Scope, query string, opts ...Option) beam.PCollection {
	o := &options{
		inputs: make(map[string]beam.PCollection),
	}
	for _, opt := range opts {
		opt(o)
	}
	if o.outType == nil {
		panic("output type must be specified for sql.Transform")
	}

	payload := beam.CrossLanguagePayload(&sqlx.ExpansionPayload{
		Query:   query,
		Dialect: o.dialect,
	})

	expansionAddr := sqlx.DefaultExpansionAddr
	if o.expansionAddr != "" {
		expansionAddr = xlangx.Require(o.expansionAddr)
	}

	out := beam.CrossLanguage(s, sqlx.Urn, payload, expansionAddr, o.inputs, beam.UnnamedOutput(o.outType))
	return out[graph.UnnamedOutputTag]
}
