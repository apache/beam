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

package validators

import (
	"fmt"
	"os"
	"testing"
)

// TestMain setups and teardown all necessary functionality for tests
// in 'validators' package (i.e. for java_validators_test, go_validators_test,
// python_validators_test)
func TestMain(m *testing.M) {
	setup()
	defer teardown()
	m.Run()
}

func setup() {
	writeFile(javaUnitTestFilePath, javaUnitTestCode)
	writeFile(javaCodePath, javaCode)
	writeFile(goUnitTestFilePath, goUnitTestCode)
	writeFile(goCodePath, goCode)
}

func teardown() {
	removeFile(javaUnitTestFilePath)
	removeFile(javaCodePath)
	removeFile(goUnitTestFilePath)
	removeFile(goCodePath)
}

func removeFile(path string) {
	err := os.Remove(path)
	if err != nil {
		panic(fmt.Errorf("error during test teardown: %s", err.Error()))
	}
}

func writeFile(path string, code string) {
	err := os.WriteFile(path, []byte(code), 0600)
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
}
