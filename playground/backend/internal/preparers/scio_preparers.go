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

import "beam.apache.org/playground/backend/internal/utils"

const (
	scioPublicClassNamePattern = "object (.*?) [{]"
	scioPackagePattern         = `^package .*$`
	scioExampleImport          = `^import com.spotify.scio.examples.*$`
	emptyStr                   = ""
)

// ScioPreparersBuilder facet of PreparersBuilder
type ScioPreparersBuilder struct {
	PreparersBuilder
}

// ScioPreparers chains to type *PreparersBuilder and returns a *ScioPreparersBuilder
func (builder *PreparersBuilder) ScioPreparers() *ScioPreparersBuilder {
	return &ScioPreparersBuilder{*builder}
}

// WithFileNameChanger adds preparer to change source code file name
func (builder *ScioPreparersBuilder) WithFileNameChanger() *ScioPreparersBuilder {
	changeNamePreparer := Preparer{
		Prepare: utils.ChangeTestFileName,
		Args:    []interface{}{builder.filePath, scioPublicClassNamePattern},
	}
	builder.AddPreparer(changeNamePreparer)
	return builder
}

// WithPackageRemover adds preparer to remove package from the code
func (builder *ScioPreparersBuilder) WithPackageRemover() *ScioPreparersBuilder {
	removePackagePreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, scioPackagePattern, ""},
	}
	builder.AddPreparer(removePackagePreparer)
	return builder
}

// WithImportRemover adds preparer to remove examples import from the code
func (builder *ScioPreparersBuilder) WithImportRemover() *ScioPreparersBuilder {
	removeImportPreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, scioExampleImport, emptyStr},
	}
	builder.AddPreparer(removeImportPreparer)
	return builder
}

// GetScioPreparers returns preparation methods that should be applied to Scio code
func GetScioPreparers(builder *PreparersBuilder) {
	builder.ScioPreparers().
		WithPackageRemover().
		WithImportRemover().
		WithFileNameChanger()
}
