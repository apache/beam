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

// Package dataframe is a wrapper for DataframeTransform defined in Apache Beam Python SDK.
// An exapnsion service for python external transforms can be started by running
// $ python -m apache_beam.runners.portability.expansion_service_main -p $PORT_FOR_EXPANSION_SERVICE
package dataframe

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*config)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*kwargs)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*argStruct)(nil)).Elem())
}

type kwargs struct {
	Fn             python.CallableSource `beam:"func"`
	IncludeIndexes bool                  `beam:"include_indexes"`
}

type argStruct struct{}

type config struct {
	dpl           kwargs
	expansionAddr string
}

type configOption func(*config)

// WithExpansionAddr sets an URL for a Python expansion service.
func WithExpansionAddr(expansionAddr string) configOption {
	return func(c *config) {
		c.expansionAddr = expansionAddr
	}
}

// WithIndexes sets include_indexes option for DataframeTransform.
func WithIndexes() configOption {
	return func(c *config) {
		c.dpl.IncludeIndexes = true
	}
}

// Transform is a multi-language wrapper for a Python DataframeTransform with a given lambda function.
// lambda function is a required parameter.
// Additional option for including indexes in dataframe can be provided by using
// dataframe.WithIndexes().
func Transform(s beam.Scope, fn string, col beam.PCollection, outT reflect.Type, opts ...configOption) beam.PCollection {
	s.Scope("xlang.python.DataframeTransform")
	cfg := config{
		dpl: kwargs{Fn: python.CallableSource(fn)},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.expansionAddr == "" {
		cfg.expansionAddr = xlangx.UseAutomatedPythonExpansionService(python.ExpansionServiceModule)
	}

	pet := python.NewExternalTransform[argStruct, kwargs]("apache_beam.dataframe.transforms.DataframeTransform")
	pet.WithKwargs(cfg.dpl)
	pl := beam.CrossLanguagePayload(pet)
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, beam.UnnamedInput(col), beam.UnnamedOutput(typex.New(outT)))
	return result[beam.UnnamedOutputTag()]

}
