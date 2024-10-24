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
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"

	"beam.apache.org/playground/backend/internal/constants"
	"beam.apache.org/playground/backend/internal/logger"
	"beam.apache.org/playground/backend/internal/utils"
)

const (
	bootstrapServerPattern            = "kafka_server:9092"
	topicNamePattern                  = "dataset"
	classWithPublicModifierPattern    = "public class "
	classWithoutPublicModifierPattern = "class "
	packagePattern                    = `^(package) (([\w]+\.)+[\w]+);`
	importStringPattern               = `import $2.*;`
	newLinePattern                    = "\n"
	javaPublicClassNamePattern        = "public class (.*?) [{|implements(.*)]"
	pipelineNamePattern               = `Pipeline\s([A-z|0-9_]*)\s=\sPipeline\.create`
	graphSavePattern                  = "String dotString = org.apache.beam.sdk.util.construction.renderer.PipelineDotRenderer.toDotString(%s);\n" +
		"    try (java.io.PrintWriter out = new java.io.PrintWriter(\"graph.dot\")) {\n      " +
		"		out.println(dotString);\n    " +
		"	} catch (java.io.FileNotFoundException e) {\n" +
		"      e.printStackTrace();\n    " +
		"\n}\n"
)

// JavaPreparersBuilder facet of PreparersBuilder
type JavaPreparersBuilder struct {
	PreparersBuilder
}

// JavaPreparers chains to type *PreparersBuilder and returns a *JavaPreparersBuilder
func (builder *PreparersBuilder) JavaPreparers() *JavaPreparersBuilder {
	return &JavaPreparersBuilder{*builder}
}

// WithPublicClassRemover adds preparer to remove public class
func (builder *JavaPreparersBuilder) WithPublicClassRemover() *JavaPreparersBuilder {
	removePublicClassPreparer := Preparer{
		Prepare: removePublicClassModifier,
		Args:    []interface{}{builder.filePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern},
	}
	builder.AddPreparer(removePublicClassPreparer)
	return builder
}

// WithPackageChanger adds preparer to change package
func (builder *JavaPreparersBuilder) WithPackageChanger() *JavaPreparersBuilder {
	changePackagePreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, packagePattern, importStringPattern},
	}
	builder.AddPreparer(changePackagePreparer)
	return builder
}

// WithPackageRemover adds preparer to remove package
func (builder *JavaPreparersBuilder) WithPackageRemover() *JavaPreparersBuilder {
	removePackagePreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, packagePattern, newLinePattern},
	}
	builder.AddPreparer(removePackagePreparer)
	return builder
}

// WithFileNameChanger adds preparer to remove package
func (builder *JavaPreparersBuilder) WithFileNameChanger() *JavaPreparersBuilder {
	unitTestFileNameChanger := Preparer{
		Prepare: utils.ChangeTestFileName,
		Args:    []interface{}{builder.filePath, javaPublicClassNamePattern},
	}
	builder.AddPreparer(unitTestFileNameChanger)
	return builder
}

// WithGraphHandler adds code to save the graph
func (builder *JavaPreparersBuilder) WithGraphHandler() *JavaPreparersBuilder {
	graphCodeAdder := Preparer{
		Prepare: addCodeToSaveGraph,
		Args:    []interface{}{builder.filePath},
	}
	builder.AddPreparer(graphCodeAdder)
	return builder
}

// WithBootstrapServersChanger adds preparer to replace tokens in the example source to correct values
func (builder *JavaPreparersBuilder) WithBootstrapServersChanger() *JavaPreparersBuilder {
	if len(builder.params) == 0 {
		return builder
	}
	bootstrapServerVal, ok := builder.params[constants.BootstrapServerKey]
	if !ok {
		return builder
	}
	bootstrapServersChanger := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, bootstrapServerPattern, bootstrapServerVal},
	}
	builder.AddPreparer(bootstrapServersChanger)
	return builder
}

// WithTopicNameChanger adds preparer to replace tokens in the example source to correct values
func (builder *JavaPreparersBuilder) WithTopicNameChanger() *JavaPreparersBuilder {
	if len(builder.params) == 0 {
		return builder
	}
	topicNameVal, ok := builder.params[constants.TopicNameKey]
	if !ok {
		return builder
	}
	topicNameChanger := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, topicNamePattern, topicNameVal},
	}
	builder.AddPreparer(topicNameChanger)
	return builder
}

func addCodeToSaveGraph(args ...interface{}) error {
	filePath := args[0].(string)
	pipelineObjectName, _ := findPipelineObjectName(filePath)
	graphSaveCode := fmt.Sprintf(graphSavePattern, pipelineObjectName)

	if pipelineObjectName != utils.EmptyLine {
		pipelineRunPosition, err := findPosition(filePath, fmt.Sprintf("(?m)^.*%s.run", pipelineObjectName))
		if err != nil {
			logger.Errorf("Cannot add graph saving code: failed to locate pipeline.run() call")
			return err
		}
		if err := insertBeforePosition(filePath, pipelineRunPosition, graphSaveCode); err != nil {
			logger.Error("Cannot add graph saving code: failed to inject code")
			return err
		}
	}
	return nil
}

// GetJavaPreparers returns preparation methods that should be applied to Java code
func GetJavaPreparers(builder *PreparersBuilder, isUnitTest bool, isKata bool) {
	if !isUnitTest && !isKata {
		builder.JavaPreparers().
			WithPublicClassRemover().
			WithPackageChanger().
			WithGraphHandler().
			WithBootstrapServersChanger().
			WithTopicNameChanger()
	}
	if isUnitTest {
		builder.JavaPreparers().
			WithPackageChanger().
			WithFileNameChanger()
	}
	if isKata {
		builder.JavaPreparers().
			WithPublicClassRemover().
			WithPackageRemover().
			WithGraphHandler().
			WithBootstrapServersChanger().
			WithTopicNameChanger()
	}
}

// findPipelineObjectName finds name of pipeline in JAVA code when pipeline creates
func findPipelineObjectName(filepath string) (string, error) {
	reg := regexp.MustCompile(pipelineNamePattern)
	b, err := ioutil.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	matches := reg.FindStringSubmatch(string(b))
	if len(matches) > 0 {
		return matches[1], nil
	} else {
		return "", nil
	}

}

// findPosition finds first line which matches the pattern
func findPosition(filePath, pattern string) (int, error) {
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		logger.Errorf("findPosition(): Error during open file: %s, err: %s", filePath, err.Error())
		return 0, err
	}

	matches := reg.FindStringIndex(string(b))
	if matches == nil {
		return 0, fmt.Errorf("cannot find location of pattern '%s' in file '%s'", pattern, filePath)
	}

	return matches[0], nil
}

func insertBeforePosition(filePath string, position int, text string) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		logger.Errorf("insertBeforePosition(): Error during open file: %s, err: %s", filePath, err.Error())
		return err
	}

	content := string(b)

	modified := content[:position] + "\n" + text + "\n" + content[position:]

	tmp, err := utils.CreateTempFile(filePath)
	if err != nil {
		logger.Errorf("insertBeforePosition(): Error during create new temporary file, err: %s", err.Error())
		return err
	}
	defer tmp.Close()

	if _, err := io.WriteString(tmp, modified); err != nil {
		logger.Errorf("insertBeforePosition(): Error during writing to temporary file, err: %s", err.Error())
		return err
	}

	// replace original file with temporary file with renaming
	if err = os.Rename(tmp.Name(), filePath); err != nil {
		logger.Errorf("insertBeforePosition(): Error during rename temporary file, err: %s", err.Error())
		return err
	}
	return nil
}

// replace processes file by filePath and replaces all patterns to newPattern
func replace(args ...interface{}) error {
	filePath := args[0].(string)
	pattern := args[1].(string)
	newPattern := args[2].(string)

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

	// uses to indicate when need to add new line to tmp file
	err = writeWithReplace(file, tmp, pattern, newPattern)
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

func removePublicClassModifier(args ...interface{}) error {
	err := replace(args...)
	return err
}

// writeWithReplace rewrites all lines from file with replacing all patterns to newPattern to another file
func writeWithReplace(from *os.File, to *os.File, pattern, newPattern string) error {
	newLine := false
	reg := regexp.MustCompile(pattern)
	scanner := bufio.NewScanner(from)

	for scanner.Scan() {
		line := scanner.Text()
		err := replaceAndWriteLine(newLine, to, line, reg, newPattern)
		if err != nil {
			logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
			return err
		}
		newLine = true
	}
	return scanner.Err()
}

// replaceAndWriteLine replaces pattern from line to newPattern and writes updated line to the file
func replaceAndWriteLine(newLine bool, to *os.File, line string, reg *regexp.Regexp, newPattern string) error {
	err := utils.AddNewLine(newLine, to)
	if err != nil {
		logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", newLinePattern, err.Error())
		return err
	}
	line = reg.ReplaceAllString(line, newPattern)
	if _, err = io.WriteString(to, line); err != nil {
		logger.Errorf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
		return err
	}
	return nil
}
