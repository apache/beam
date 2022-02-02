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

package preparers_utils

import (
	"beam.apache.org/playground/backend/internal/logger"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	indentationReplacement = "$0"
	EmptyLine              = ""
	GraphFileName          = "graph.dot"
	pythonGraphCodePattern = "$0# Write graph to file\n$0from apache_beam.runners.interactive.display import pipeline_graph\n$0dot = pipeline_graph.PipelineGraph(%s).get_dot()\n$0with open('%s', 'w') as file:\n$0  file.write(dot)\n"
	newLinePattern         = "\n"
)

type PipelineDefinitionType int

const (
	RegularDefinition PipelineDefinitionType = 0
	WithDefinition    PipelineDefinitionType = 1
)

// DefineVars creates empty variables
func DefineVars() (string, string, error, bool, PipelineDefinitionType) {
	return EmptyLine, EmptyLine, errors.New(EmptyLine), false, RegularDefinition
}

// AddGraphToEndOfFile if no place for graph was found adds graph code to the end of the file
func AddGraphToEndOfFile(spaces string, err error, tempFile *os.File, pipelineName string) {
	line := EmptyLine
	regs := []*regexp.Regexp{regexp.MustCompile("^")}
	_, err = Wrap(addGraphCode)(tempFile, &line, &spaces, &pipelineName, &regs)
}

// ProcessLine process the current line from the file by either:
// - trying to find the definition of the beam pipeline and save the pipeline name
// - or trying to find the place where to add the code for the graph saving and adds it to the line
// after it save the line to the temp file
func ProcessLine(curLine string, pipelineName *string, spaces *string, regs *[]*regexp.Regexp, tempFile *os.File, err error) (bool, PipelineDefinitionType, error) {
	done := false
	definitionType := RegularDefinition
	if *pipelineName == "" {
		// Try tempFile find where the beam pipeline name is defined
		definitionType, err = Wrap(getPipelineName)(tempFile, &curLine, spaces, pipelineName, regs)
	} else {
		// Try tempFile find where beam pipeline definition is finished and add code tempFile store the graph
		_, err = Wrap(addGraphCode)(tempFile, &curLine, spaces, pipelineName, regs)
		if *regs == nil {
			done = true
		}
	}
	return done, PipelineDefinitionType(definitionType), err
}

//getPipelineName looking for a declaration of a beam pipeline and it's name
func getPipelineName(line, spaces, pipelineName *string, regs *[]*regexp.Regexp) PipelineDefinitionType {
	for i, reg := range *regs {
		found := (*reg).FindAllStringSubmatch(*line, -1)
		if found != nil {
			*spaces = found[0][1]
			*pipelineName = found[0][2]
			*regs = nil
			return PipelineDefinitionType(i)
		}
	}
	return 0
}

//addGraphCode adds line for the graph saving to specific place in the code
func addGraphCode(line, spaces, pipelineName *string, regs *[]*regexp.Regexp) PipelineDefinitionType {
	for i, reg := range *regs {
		found := (*reg).FindAllStringSubmatch(*line, -1)
		if found != nil {
			graphCode := fmt.Sprintf(pythonGraphCodePattern, *pipelineName, GraphFileName)
			graphCodeWithIndentation := strings.ReplaceAll(graphCode, indentationReplacement, *spaces)
			*line = graphCodeWithIndentation + *line
			*regs = nil
			return PipelineDefinitionType(i)
		}
	}
	return 0
}

func Pass(_, _, _ *string, _ *[]*regexp.Regexp) PipelineDefinitionType {
	// No need to find or change anything, just use Wrap method
	return RegularDefinition
}

type WrappedFunction func(line, spaces, pipelineName *string, regs *[]*regexp.Regexp) PipelineDefinitionType

// Wrap decorator that writes new line to temp file and a line received from "getLine" method.
func Wrap(wrappedFunction WrappedFunction) func(to *os.File, line, spaces, pipelineName *string, regs *[]*regexp.Regexp) (PipelineDefinitionType, error) {
	return func(to *os.File, line, spaces, pipelineName *string, regs *[]*regexp.Regexp) (PipelineDefinitionType, error) {
		err := AddNewLine(true, to)
		if err != nil {
			return 0, err
		}
		foundPattern := wrappedFunction(line, spaces, pipelineName, regs)
		if _, err := io.WriteString(to, *line); err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
			return 0, err
		}
		return foundPattern, nil
	}
}

// AddNewLine adds a new line at the end of the file
func AddNewLine(newLine bool, file *os.File) error {
	if !newLine {
		return nil
	}
	if _, err := io.WriteString(file, newLinePattern); err != nil {
		return err
	}
	return nil
}

// GetPublicClassName return the name of main public class in the file
func GetPublicClassName(filePath, pattern string) (string, error) {
	code, err := ioutil.ReadFile(filePath)
	if err != nil {
		logger.Errorf("Preparer: Error during open file: %s, err: %s\n", filePath, err.Error())
		return "", err
	}
	re := regexp.MustCompile(pattern)
	className := re.FindStringSubmatch(string(code))[1]
	return className, err
}

// RenameSourceCodeFile renames the name of the file (for example from pipelineId.scala to MinimalWordCount.scala)
func RenameSourceCodeFile(filePath string, className string) error {
	currentFileName := filepath.Base(filePath)
	newFilePath := strings.Replace(filePath, currentFileName, fmt.Sprintf("%s%s", className, filepath.Ext(currentFileName)), 1)
	err := os.Rename(filePath, newFilePath)
	return err
}
