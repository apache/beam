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

// Package inference has the cross language implementation of RunInference API implemented in Python SDK.
// An exapnsion service for python external transforms can be started by running
//
//	$ python -m apache_beam.runners.portability.expansion_service_main -p $PORT_FOR_EXPANSION_SERVICE
package inference

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*sklearnConfig)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*argsStruct)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*sklearnKwargs)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*PredictionResult)(nil)).Elem())
}

var outputT = reflect.TypeOf((*PredictionResult)(nil)).Elem()

// PredictionResult represents the result of a prediction obtained from Python's RunInference API.
type PredictionResult struct {
	Example   []int64 `beam:"example"`
	Inference int32   `beam:"inference"`
}

type sklearnConfig struct {
	kwargs        sklearnKwargs
	args          argsStruct
	expansionAddr string
}

type sklearnConfigOption func(*sklearnConfig)

// Sets arguments for the python transform parameters
func WithArgs(args []string) sklearnConfigOption {
	return func(c *sklearnConfig) {
		c.args.args = append(c.args.args, args...)
	}
}

// WithExpansionAddr provides URL for Python expansion service.
func WithExpansionAddr(expansionAddr string) sklearnConfigOption {
	return func(c *sklearnConfig) {
		c.expansionAddr = expansionAddr
	}
}

type argsStruct struct {
	args []string
}

// sklearnKwargs defines acceptable keyword args for Sklearn Model Handler.
type sklearnKwargs struct {
	// ModelHandlerProvider defines the model handler to be used.
	ModelHandlerProvider python.CallableSource `beam:"model_handler_provider"`
	// ModelURI indicates the model path to be used for Sklearn Model Handler.
	ModelURI string `beam:"model_uri"`
}

// Sklearn provides inference over a SklearnModelHandler.
// ModelURI is the required parameter indicating the path to the sklearn model.
// This wrapper doesn't work for keyed input PCollection.
//
// Example:
//		inputRow := [][]int64{{0, 0}, {1, 1}}
//	    input := beam.CreateList(s, inputRow)
//	    modelURI = gs://example.com/tmp/staged/sklearn_model
//		predictions := inference.Sklearn(s, modelURI, input, inference.WithExpansionAddr(expansionAddr))
func Sklearn(s beam.Scope, modelUri string, col beam.PCollection, opts ...sklearnConfigOption) beam.PCollection {
	s.Scope("xlang.inference.Sklearn")

	cfg := sklearnConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.kwargs.ModelHandlerProvider = python.CallableSource("apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy")
	cfg.kwargs.ModelURI = modelUri
	return runInference[sklearnKwargs](s, col, cfg.args, cfg.kwargs, cfg.expansionAddr)
}

func runInference[Kwargs any](s beam.Scope, col beam.PCollection, a argsStruct, k Kwargs, addr string) beam.PCollection {
	expansionAddr := addr
	if expansionAddr == "" {
		expansionAddr = xlangx.UseAutomatedPythonExpansionService(python.ExpansionServiceModule)
	}
	pet := python.NewExternalTransform[argsStruct, Kwargs]("apache_beam.ml.inference.base.RunInference.from_callable")
	pet.WithKwargs(k)
	pet.WithArgs(a)
	pl := beam.CrossLanguagePayload(pet)
	namedInput := map[string]beam.PCollection{xlang.SetOutputCoder: col}
	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, expansionAddr, namedInput, beam.UnnamedOutput(typex.New(outputT)))
	return result[beam.UnnamedOutputTag()]
}
