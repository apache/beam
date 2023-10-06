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
// An expansion service for python external transforms can be started by running
//
//	$ python -m apache_beam.runners.portability.expansion_service_main -p $PORT_FOR_EXPANSION_SERVICE
package inference

import (
	"context"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/xlang/python"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*runInferenceConfig)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*argsStruct)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*sklearn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*PredictionResult)(nil)).Elem())
}

var outputT = reflect.TypeOf((*PredictionResult)(nil)).Elem()

// PredictionResult represents the result of a prediction obtained from Python's RunInference API.
type PredictionResult struct {
	Example   []int64 `beam:"example"`
	Inference int32   `beam:"inference"`
}

type runInferenceConfig struct {
	args          argsStruct
	expansionAddr string
	extraPackages []string
}

type runInferenceOption func(*runInferenceConfig)

// WithArgs set arguments for the RunInference transform parameters.
func WithArgs(args []string) runInferenceOption {
	return func(c *runInferenceConfig) {
		c.args.args = append(c.args.args, args...)
	}
}

// WithExpansionAddr provides URL for Python expansion service.
func WithExpansionAddr(expansionAddr string) runInferenceOption {
	return func(c *runInferenceConfig) {
		c.expansionAddr = expansionAddr
	}
}

// WithExtraPackages is used to specify additional packages when using an automated expansion service.
// Packages required to run the required Model are included implicitly,
// eg: scikit-learn, pandas for Sklearn Model Handler.
func WithExtraPackages(extraPackages []string) runInferenceOption {
	return func(c *runInferenceConfig) {
		c.extraPackages = extraPackages
	}
}

type argsStruct struct {
	args []string
}

func inferExtraPackages(modelHandler string) []string {
	extraPackages := []string{}

	mhLowered := strings.ToLower(modelHandler)
	if strings.Contains(mhLowered, "sklearn") {
		extraPackages = append(extraPackages, "scikit-learn", "pandas")
	} else if strings.Contains(mhLowered, "pytorch") {
		extraPackages = append(extraPackages, "torch")
	}
	if len(extraPackages) > 0 {
		log.Infof(context.Background(), "inferExtraPackages: %v", extraPackages)
	}
	return extraPackages
}

// sklearn configures the parameters for the sklearn inference transform.
type sklearn struct {
	// ModelHandlerProvider defines the model handler to be used.
	ModelHandlerProvider python.CallableSource `beam:"model_handler_provider"`
	// ModelURI indicates the model path to be used for Sklearn Model Handler.
	ModelURI string `beam:"model_uri"`
}

// sklearnConfig could be used to configure other optional parameters in future if necessary.
type sklearnConfig func(*sklearn)

// SklearnModel configures the parameters required to perform RunInference transform
// on Sklearn Model. It returns an sklearn object which should be used to call
// RunInference transform.
// ModelURI is the required parameter indicating the path to the sklearn model.
//
//	Example:
//	  modelURI := "gs://storage/model"
//	  model := inference.SklearnModel(modelURI)
//	  prediction := model.RunInference(s, input, inference.WithExpansionAddr("localhost:9000"))
func SklearnModel(modelURI string, opts ...sklearnConfig) sklearn {
	sm := sklearn{
		ModelHandlerProvider: python.CallableSource("apache_beam.ml.inference.sklearn_inference.SklearnModelHandlerNumpy"),
		ModelURI:             modelURI,
	}
	for _, opt := range opts {
		opt(&sm)
	}
	return sm
}

// RunInference transforms the input pcollection by calling RunInference in Python SDK
// using Sklearn Model Handler with python expansion service.
// ExpansionAddress can be provided by using inference.WithExpansionAddr(address).
// NOTE: This wrapper doesn't work for keyed input PCollection.
//
//	  Example:
//			inputRow := [][]int64{{0, 0}, {1, 1}}
//		    input := beam.CreateList(s, inputRow)
//		    modelURI = gs://example.com/tmp/staged/sklearn_model
//	        model := inference.SklearnModel(modelURI)
//	        prediction := model.RunInference(s, input, inference.WithExpansionAddr("localhost:9000"))
func (sk sklearn) RunInference(s beam.Scope, col beam.PCollection, opts ...runInferenceOption) beam.PCollection {
	s.Scope("xlang.inference.sklearn.RunInference")

	cfg := runInferenceConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.expansionAddr == "" {
		cfg.extraPackages = append(cfg.extraPackages, inferExtraPackages(string(sk.ModelHandlerProvider))...)
	}

	return runInference[sklearn](s, col, sk, cfg)
}

func runInference[Kwargs any](s beam.Scope, col beam.PCollection, k Kwargs, cfg runInferenceConfig) beam.PCollection {
	if cfg.expansionAddr == "" {
		if len(cfg.extraPackages) > 0 {
			cfg.expansionAddr = xlangx.UseAutomatedPythonExpansionService(python.ExpansionServiceModule, xlangx.AddExtraPackages(cfg.extraPackages))
		} else {
			cfg.expansionAddr = xlangx.UseAutomatedPythonExpansionService(python.ExpansionServiceModule)
		}
	}
	pet := python.NewExternalTransform[argsStruct, Kwargs]("apache_beam.ml.inference.base.RunInference.from_callable")
	pet.WithKwargs(k)
	pet.WithArgs(cfg.args)
	pl := beam.CrossLanguagePayload(pet)

	// Since External RunInference Transform with Python Expansion Service will send encoded output, we need to specify
	// output coder. We do this by setting the output tag as xlang.SetOutputCoder so that while sending
	// an expansion request we populate the OutputCoderRequests field. If this is not done then the encoded output
	// may not be decoded with coders known to Go SDK.
	outputType := map[string]typex.FullType{xlang.SetOutputCoder: typex.New(outputT)}

	result := beam.CrossLanguage(s, "beam:transforms:python:fully_qualified_named", pl, cfg.expansionAddr, beam.UnnamedInput(col), outputType)
	return result[beam.UnnamedOutputTag()]
}
