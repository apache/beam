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
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

const (
	addLogHandlerCode      = "import logging\nlogging.basicConfig(\n    level=logging.DEBUG,\n    format=\"%(asctime)s [%(levelname)s] %(message)s\",\n    handlers=[\n        logging.FileHandler(\"logs.log\"),\n    ]\n)\n"
	oneIndentation         = "  "
	indentationReplacement = "$0"
	findPipelinePattern    = `(\s+)with.+Pipeline.+as (.+):`
	indentationPattern     = `^(%s){0,1}\w+`
	GraphFileName          = "graph.dot"
	graphCodePattern       = "$0# Write graph to file\n$0from apache_beam.runners.interactive.display import pipeline_graph\n$0dot = pipeline_graph.PipelineGraph(%s).get_dot()\n$0with open('%s', 'w') as file:\n$0  file.write(dot)\n"
)

// GetPythonPreparers returns preparation methods that should be applied to Python code
func GetPythonPreparers(builder *PreparersBuilder, isUnitTest bool) {
	builder.
		PythonPreparers().
		WithLogHandler()
	if !isUnitTest {
		builder.PythonPreparers().WithGraphHandler()
	}
}

//PythonPreparersBuilder facet of PreparersBuilder
type PythonPreparersBuilder struct {
	PreparersBuilder
}

//PythonPreparers chains to type *PreparersBuilder and returns a *GoPreparersBuilder
func (builder *PreparersBuilder) PythonPreparers() *PythonPreparersBuilder {
	return &PythonPreparersBuilder{*builder}
}

//WithLogHandler adds code for logging
func (builder *PythonPreparersBuilder) WithLogHandler() *PythonPreparersBuilder {
	addLogHandler := Preparer{
		Prepare: addCodeToFile,
		Args:    []interface{}{builder.filePath, writeLogCodeToFile},
	}
	builder.AddPreparer(addLogHandler)
	return builder
}

//WithGraphHandler adds code to save the graph
func (builder *PythonPreparersBuilder) WithGraphHandler() *PythonPreparersBuilder {
	addLogHandler := Preparer{
		Prepare: addCodeToFile,
		Args:    []interface{}{builder.filePath, saveGraph},
	}
	builder.AddPreparer(addLogHandler)
	return builder
}

// addCodeToFile processes file by filePath and adds additional code
func addCodeToFile(args ...interface{}) error {
	filePath := args[0].(string)
	addCodeMethod := args[1].(func(*os.File, *os.File) error)

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

	err = addCodeMethod(file, tmp)
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

// writeLogCodeToFile rewrites all lines from file with adding additional code to another file
// New code is added to the top of the file.
func writeLogCodeToFile(from *os.File, to *os.File) error {
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

// saveGraph add code to pipeline to save the pipeline's graph to the file graph.dot
func saveGraph(from *os.File, to *os.File) error {
	newLine := false
	reg := regexp.MustCompile(findPipelinePattern)
	scanner := bufio.NewScanner(from)
	pipelineName := ""
	spaces := ""
	err := errors.New("")

	for scanner.Scan() {
		line := scanner.Text()
		if pipelineName == "" {
			// Try to find where the beam pipeline declaration is located
			err = writeLineToFile(findPipelineLine)(newLine, to, &line, &spaces, &pipelineName, &reg)
		} else if reg != nil {
			// Try to find where beam pipeline definition is finished and add code to store the graph
			reg = regexp.MustCompile(fmt.Sprintf(indentationPattern, spaces))
			err = writeLineToFile(addCodeForGraph)(newLine, to, &line, &spaces, &pipelineName, &reg)
		} else {
			err = writeLineToFile(func(line, spaces, pipelineName *string, reg **regexp.Regexp) {
				// No need to find or change anything, just write current line to file
			})(newLine, to, &line, &spaces, &pipelineName, nil)
		}
		if err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
			return err
		}
		newLine = true
	}
	return scanner.Err()
}

//findPipelineLine looking for a declaration of a beam pipeline and it's name
func findPipelineLine(line, spaces, pipelineName *string, reg **regexp.Regexp) {
	found := (*reg).FindAllStringSubmatch(*line, -1)
	if found != nil {
		*spaces = found[0][1]
		*pipelineName = found[0][2]
	}
}

//addCodeForGraph adds line for the graph saving to specific place in code
func addCodeForGraph(line, spaces, pipelineName *string, reg **regexp.Regexp) {
	found := (*reg).FindAllStringSubmatch(*line, -1)
	if found != nil {
		indentation := *spaces + oneIndentation
		graphCode := fmt.Sprintf(graphCodePattern, *pipelineName, GraphFileName)
		graphCodeWithIndentation := strings.ReplaceAll(graphCode, indentationReplacement, indentation)
		*line = graphCodeWithIndentation + *line
		*reg = nil
	}
}

type GetLine func(line, spaces, pipelineName *string, reg **regexp.Regexp)
type Decorated func(newLine bool, to *os.File, line, spaces, pipelineName *string, reg **regexp.Regexp) error

// writeLineToFile decorator that writes new line to temp file and a line received from "getLine" method.
func writeLineToFile(getLine GetLine) Decorated {
	return func(newLine bool, to *os.File, line, spaces, pipelineName *string, reg **regexp.Regexp) error {
		err := addNewLine(newLine, to)
		if err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", newLinePattern, err.Error())
			return err
		}
		getLine(line, spaces, pipelineName, reg)
		if _, err = io.WriteString(to, *line); err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
			return err
		}
		return nil
	}
}
