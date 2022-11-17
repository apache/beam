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

//go:generate go install github.com/apache/beam/sdks/v2/go/cmd/specialize
//go:generate specialize --package=register --input=register.tmpl --x=data,universals --imports=typex
//go:generate go fmt

/*
Package register contains functions for registering and optimizing your DoFn.

This package contains generic register/optimization function for each possible combination of input and output arities in a DoFn's ProcessElement function.
For example, given a DoFn with a ProcessElement function that takes 4 inputs and returns 3 outputs, you can call
register.DoFn4x3[input1 type, input2 type, input3 type, input4 type, output1 type, output2 type, output3 type](&doFn{}) during pipeline construction. This will
register your DoFn and produce optimized callers for your DoFn to significantly speed up execution at runtime.

See DoFn3x1 for a full example.
*/
package register
