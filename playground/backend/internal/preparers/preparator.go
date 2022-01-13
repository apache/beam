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

package preparers

// Preparer is used to make preparations with file with code.
type Preparer struct {
	Prepare func(args ...interface{}) error
	Args    []interface{}
}

type Preparers struct {
	functions *[]Preparer
}

func (preparers *Preparers) GetPreparers() *[]Preparer {
	return preparers.functions
}

//PreparersBuilder struct
type PreparersBuilder struct {
	preparers *Preparers
	filePath  string
}

//NewPreparersBuilder constructor for PreparersBuilder
func NewPreparersBuilder(filePath string) *PreparersBuilder {
	return &PreparersBuilder{preparers: &Preparers{functions: &[]Preparer{}}, filePath: filePath}
}

//Build builds preparers from PreparersBuilder
func (builder *PreparersBuilder) Build() *Preparers {
	return builder.preparers
}

func (builder *PreparersBuilder) AddPreparer(newPreparer Preparer) {
	*builder.preparers.functions = append(*builder.preparers.functions, newPreparer)
}
