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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type WrongExtension struct {
	error string
}

func (e *WrongExtension) Error() string {
	return fmt.Sprintf("File has wrong extension: %v", e.error)
}

// isNotExist checks if file exists or not and returns error is file doesn't exist
func isNotExist(filePath string) bool {
	_, err := os.Stat(filePath)
	return errors.Is(err, fs.ErrNotExist)
}

// isCorrectExtension checks if the file has correct extension (.java, .go, .py)
func isCorrectExtension(filePath string, correctExtension string) bool {
	fileExtension := filepath.Ext(filePath)
	return strings.EqualFold(fileExtension, correctExtension)
}

// CheckPathIsValid checks that the file exists and has a correct extension
func CheckPathIsValid(args ...interface{}) (bool, error) {
	filePath := args[0].(string)
	correctExtension := args[1].(string)
	notExists := isNotExist(filePath)
	if notExists {
		return false, fs.ErrNotExist
	}
	if !isCorrectExtension(filePath, correctExtension) {
		return false, &WrongExtension{fmt.Sprintf("expected extension %s", correctExtension)}
	}
	return true, nil
}
