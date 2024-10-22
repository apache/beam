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
	"beam.apache.org/playground/backend/internal/utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
)

const (
	addLogHandlerCode       = "import logging\nlogging.basicConfig(\n    level=logging.ERROR,\n    format=\"%(asctime)s [%(levelname)s] %(message)s\",\n    handlers=[\n        logging.FileHandler(\"logs.log\"),\n    ]\n)\n"
	oneIndentation          = "  "
	findWithPipelinePattern = `(\s*)with.+Pipeline.+as (.+):`
	indentationPattern      = `^(%s){0,1}\w+`
	findPipelinePattern     = `^(\s*)(.+) = beam.Pipeline`
	runPipelinePattern      = `^(\s*).*%s.run\(\)`
)

// GetPythonPreparers returns preparation methods that should be applied to Python code
func GetPythonPreparers(builder *PreparersBuilder, isUnitTest bool) {
	builder.
		PythonPreparers().
		WithLogHandler()
	if !isUnitTest {
		builder.
			PythonPreparers().
			WithGraphHandler()
	}
}

// PythonPreparersBuilder facet of PreparersBuilder
type PythonPreparersBuilder struct {
	PreparersBuilder
}

// PythonPreparers chains to type *PreparersBuilder and returns a *GoPreparersBuilder
func (builder *PreparersBuilder) PythonPreparers() *PythonPreparersBuilder {
	return &PythonPreparersBuilder{*builder}
}

// WithLogHandler adds code for logging
func (builder *PythonPreparersBuilder) WithLogHandler() *PythonPreparersBuilder {
	addLogHandler := Preparer{
		Prepare: addCodeToFile,
		Args:    []interface{}{builder.filePath, saveLogs},
	}
	builder.AddPreparer(addLogHandler)
	return builder
}

// WithGraphHandler adds code to save the graph
func (builder *PythonPreparersBuilder) WithGraphHandler() *PythonPreparersBuilder {
	addGraphHandler := Preparer{
		Prepare: addCodeToFile,
		Args:    []interface{}{builder.filePath, saveGraph},
	}
	builder.AddPreparer(addGraphHandler)
	return builder
}

// addCodeToFile processes file by filePath and adds additional code
func addCodeToFile(args ...interface{}) error {
	filePath := args[0].(string)
	methodToAddCode := args[1].(func(*os.File, *os.File) error)

	file, err := os.Open(filePath)
	if err != nil {
		logger.Errorf("Preparation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return err
	}
	defer file.Close()

	tmp, err := utils.CreateTempFile(filePath)
	if err != nil {
		logger.Errorf("Preparation: Error during create new temporary file, err: %s\n", err.Error())
		return err
	}
	defer tmp.Close()

	err = methodToAddCode(file, tmp)
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

// saveLogs rewrites all lines from file with adding additional code to another file
// New code is added to the top of the file.
func saveLogs(from *os.File, to *os.File) error {
	if err := writeToFile(to, addLogHandlerCode); err != nil {
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

// saveGraph adds code to pipeline to save the pipeline's graph to the file GraphFileName
func saveGraph(from *os.File, tempFile *os.File) error {
	regs := []*regexp.Regexp{
		regexp.MustCompile(findPipelinePattern),
		regexp.MustCompile(findWithPipelinePattern),
	}
	scanner := bufio.NewScanner(from)
	pipelineName, spaces, err, done, definitionType := utils.InitVars()

	for scanner.Scan() {
		line := scanner.Text()
		if !done {
			done, definitionType, err = utils.ProcessLine(line, &pipelineName, &spaces, &regs, tempFile, err)
			if pipelineName != "" && regs == nil { // if the pipeline name is found then we know the pipeline name and regex is set to nil then
				switch definitionType { // we define the next regex to find where pipeline is `run` or `with` statement is closed
				case utils.RegularDefinition:
					regs = []*regexp.Regexp{
						regexp.MustCompile(fmt.Sprintf(runPipelinePattern, pipelineName)),
					}
				case utils.WithDefinition:
					regs = []*regexp.Regexp{
						regexp.MustCompile(fmt.Sprintf(indentationPattern, spaces)),
					}
					spaces = spaces + oneIndentation
				}
			}
		} else {
			// No need to find or add anything, just add current `line` to file
			err = utils.AddNewLine(true, tempFile)
			if err == nil {
				if _, err = io.WriteString(tempFile, line); err != nil {
					logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
				}
			}
		}
		if err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" tempFile tmp file, err: %s\n", line, err.Error())
			return err
		}
	}
	if !done && pipelineName != "" {
		utils.AddGraphToEndOfFile(spaces, err, tempFile, pipelineName)
	}
	return scanner.Err()
}
