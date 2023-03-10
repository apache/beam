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

type scioPreparer struct {
	preparer
}

func GetScioPreparer(filePath string) Preparer {
	return scioPreparer{
		preparer{
			filePath: filePath,
		},
	}
}

func (p scioPreparer) Prepare() error {
	// Remove 'package' string
	if err := replace(p.filePath, scioPackagePattern, ""); err != nil {
		return err
	}
	// Remove imports
	if err := replace(p.filePath, scioExampleImport, emptyStr); err != nil {
		return err
	}
	// Rename file to match main file name
	if err := utils.ChangeTestFileName(p.filePath, scioPublicClassNamePattern); err != nil {
		return err
	}
	return nil
}
