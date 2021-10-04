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

// Package fs_tool utils for checking the valid file path
package fs_tool

import (
	"os"
	"path/filepath"
	"strings"
)

// IsExist checks if file exists
func IsExist(filePath string) (bool, error) {
	if _, err := os.Stat(filePath); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

// IsCorrectExtension checks if the file has correct extension (.java, .go, .py)
func IsCorrectExtension(filePath string, correctExtension string) bool {
	fileExtension := filepath.Ext(filePath)
	return strings.EqualFold(fileExtension, correctExtension)
}

// CheckPathIsValid checks that the file exists and has a correct extension
func CheckPathIsValid(filePath string, correctExtension string) (bool, error) {
	exists, err := IsExist(filePath)
	if err == nil {
		return exists && IsCorrectExtension(filePath, correctExtension), nil
	} else {
		return false, err
	}
}
