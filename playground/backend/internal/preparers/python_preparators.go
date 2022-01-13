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

import (
	"beam.apache.org/playground/backend/internal/logger"
	"bufio"
	"io"
	"os"
)

const (
	addLogHandlerCode = "import logging\nlogging.basicConfig(\n    level=logging.DEBUG,\n    format=\"%(asctime)s [%(levelname)s] %(message)s\",\n    handlers=[\n        logging.FileHandler(\"logs.log\"),\n    ]\n)\n"
)

// GetPythonPreparers returns preparation methods that should be applied to Python code
func GetPythonPreparers(builder *PreparersBuilder) {
	builder.
		PythonPreparers().
		WithLogHandler()
}

//PythonPreparersBuilder facet of PreparersBuilder
type PythonPreparersBuilder struct {
	PreparersBuilder
}

//PythonPreparers chains to type *PreparersBuilder and returns a *GoPreparersBuilder
func (b *PreparersBuilder) PythonPreparers() *PythonPreparersBuilder {
	return &PythonPreparersBuilder{*b}
}

//WithLogHandler adds code for logging
func (a *PythonPreparersBuilder) WithLogHandler() *PythonPreparersBuilder {
	addLogHandler := Preparer{
		Prepare: addCodeToFile,
		Args:    []interface{}{a.filePath, addLogHandlerCode},
	}
	a.AddPreparer(addLogHandler)
	return a
}

// addCodeToFile processes file by filePath and adds additional code
func addCodeToFile(args ...interface{}) error {
	filePath := args[0].(string)
	additionalCode := args[1].(string)

	file, err := os.Open(filePath)
	if err != nil {
		logger.Errorf("Preparation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return err
	}
	defer file.Close()

	tmp, err := createTempFile(filePath)
	if err != nil {
		logger.Errorf("Preparation: Error during create new temporary file, err: %s\n", err.Error())
		return err
	}
	defer tmp.Close()

	err = writeCodeToFile(file, tmp, additionalCode)
	if err != nil {
		logger.Errorf("Preparation: Error during write data to tmp file, err: %s\n", err.Error())
		return err
	}

	// replace original file with temporary file with renaming
	if err = os.Rename(tmp.Name(), filePath); err != nil {
		logger.Errorf("Preparation: Error during rename temporary file, err: %s\n", err.Error())
		return err
	}
	return nil
}

// writeCodeToFile rewrites all lines from file with adding additional code to another file
// New code is added to the top of the file.
func writeCodeToFile(from *os.File, to *os.File, code string) error {
	if err := writeToFile(to, code); err != nil {
		return err
	}

	scanner := bufio.NewScanner(from)
	for scanner.Scan() {
		line := scanner.Text()

		if err := writeToFile(to, line+"\n"); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// writeToFile writes str to the file.
func writeToFile(to *os.File, str string) error {
	if _, err := io.WriteString(to, str); err != nil {
		logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", str, err.Error())
		return err
	}
	return nil
}
