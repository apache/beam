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

package preparators

// Preparator is used to make preparations with file with code.
type Preparator struct {
	Prepare func(args ...interface{}) error
	Args    []interface{}
}

type Preparers struct {
	preparersFunctions *[]Preparator
}

func (p *Preparers) GetPreparers() *[]Preparator {
	return p.preparersFunctions
}

//PreparersBuilder struct
type PreparersBuilder struct {
	preparers *Preparers
	filePath  string
}

//NewPreparersBuilder constructor for PreparersBuilder
func NewPreparersBuilder(filePath string) *PreparersBuilder {
	return &PreparersBuilder{preparers: &Preparers{preparersFunctions: &[]Preparator{}}, filePath: filePath}
}

//Build builds preparers from PreparersBuilder
func (b *PreparersBuilder) Build() *Preparers {
	return b.preparers
}

func (p *PreparersBuilder) AddPreparer(newPreparer Preparator) {
	*p.preparers.preparersFunctions = append(*p.preparers.preparersFunctions, newPreparer)
}
