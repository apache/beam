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

package utils

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"beam.apache.org/playground/backend/internal/logger"
)

const (
	indentationReplacement = "$0"
	EmptyLine              = ""
	GraphFileName          = "graph.dot"
	pythonGraphCodePattern = "$0# Write graph to file\n$0from apache_beam.runners.interactive.display import pipeline_graph\n$0dot = pipeline_graph.PipelineGraph(%s).get_dot()\n$0with open('%s', 'w') as file:\n$0  file.write(dot)\n"
	newLinePattern         = "\n"
	tmpFileSuffix          = "tmp"
)

type PipelineDefinitionType int

const (
	RegularDefinition PipelineDefinitionType = 0 // the definition of python pipeline like: "p = beam.Pipeline(some_options)"
	WithDefinition    PipelineDefinitionType = 1 // the definition of python pipeline like: "with beam.Pipeline(some_options) as p:"
)

// ReplaceSpacesWithEquals prepares pipelineOptions by replacing spaces between option and them value to equals.
func ReplaceSpacesWithEquals(pipelineOptions string) string {
	re := regexp.MustCompile(`(--[A-z0-9]+)\s([A-z0-9]+)`)
	return re.ReplaceAllString(pipelineOptions, "$1=$2")
}

// InitVars creates empty variables
func InitVars() (string, string, error, bool, PipelineDefinitionType) {
	return EmptyLine, EmptyLine, errors.New(EmptyLine), false, RegularDefinition
}

// AddGraphToEndOfFile if no place for graph was found adds graph code to the end of the file
func AddGraphToEndOfFile(spaces string, err error, tempFile *os.File, pipelineName string) {
	line := EmptyLine
	regs := []*regexp.Regexp{regexp.MustCompile("^")}
	_, err = wrap(addGraphCode)(tempFile, &line, &spaces, &pipelineName, &regs)
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
		definitionType, err = wrap(getVarName)(tempFile, &curLine, spaces, pipelineName, regs)
	} else {
		// Try tempFile find where beam pipeline definition is finished and add code tempFile store the graph
		_, err = wrap(addGraphCode)(tempFile, &curLine, spaces, pipelineName, regs)
		if *regs == nil {
			done = true
		}
	}
	return done, definitionType, err
}

// getVarName looking for a declaration of a beam pipeline and it's name
func getVarName(line, spaces, pipelineName *string, regs *[]*regexp.Regexp) PipelineDefinitionType {
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

// addGraphCode adds line for the graph saving to specific place in the code
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

type WrappedFunction func(line, spaces, pipelineName *string, regs *[]*regexp.Regexp) PipelineDefinitionType

// wrap decorator that writes new line to temp file and a line received from "getLine" method.
func wrap(wrappedFunction WrappedFunction) func(to *os.File, line, spaces, pipelineName *string, regs *[]*regexp.Regexp) (PipelineDefinitionType, error) {
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
	classNameMatch := re.FindStringSubmatch(string(code))
	if len(classNameMatch) == 0 {
		return "", errors.New(fmt.Sprintf("unable to find main class name in file %s", filePath))
	}

	className := classNameMatch[1]
	return className, err
}

// RenameSourceCodeFile renames the name of the file (for example from pipelineId.scala to MinimalWordCount.scala)
func RenameSourceCodeFile(filePath string, className string) error {
	currentFileName := filepath.Base(filePath)
	newFilePath := strings.Replace(filePath, currentFileName, fmt.Sprintf("%s%s", className, filepath.Ext(currentFileName)), 1)
	err := os.Rename(filePath, newFilePath)
	return err
}

// CreateTempFile creates temporary file next to originalFile
func CreateTempFile(originalFilePath string) (*os.File, error) {
	fileName := filepath.Base(originalFilePath)
	tmpFileName := fmt.Sprintf("%s_%s", tmpFileSuffix, fileName)
	tmpFilePath := strings.Replace(originalFilePath, fileName, tmpFileName, 1)
	return os.Create(tmpFilePath)
}

func ChangeTestFileName(args ...interface{}) error {
	filePath := args[0].(string)
	publicClassNamePattern := args[1].(string)
	className, err := GetPublicClassName(filePath, publicClassNamePattern)
	if err != nil {
		return err
	}
	err = RenameSourceCodeFile(filePath, className)
	if err != nil {
		return err
	}
	return nil
}
