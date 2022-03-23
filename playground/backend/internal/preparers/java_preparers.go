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
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

const (
	classWithPublicModifierPattern    = "public class "
	classWithoutPublicModifierPattern = "class "
	packagePattern                    = `^(package) (([\w]+\.)+[\w]+);`
	importStringPattern               = `import $2.*;`
	newLinePattern                    = "\n"
	pathSeparatorPattern              = os.PathSeparator
	tmpFileSuffix                     = "tmp"
	javaPublicClassNamePattern        = "public class (.*?) [{|implements(.*)]"
	pipelineNamePattern               = `Pipeline\s([A-z|0-9_]*)\s=\sPipeline\.create`
	graphSavePattern                  = "String dotString = org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer.toDotString(%s);\n" +
		"    try (java.io.PrintWriter out = new java.io.PrintWriter(\"%s\")) {\n      " +
		"		out.println(dotString);\n    " +
		"	} catch (java.io.FileNotFoundException e) {\n" +
		"      e.printStackTrace();\n    " +
		"\n}\n"
	graphRunPattern = "(.*%s.run.*;)"
)

//JavaPreparersBuilder facet of PreparersBuilder
type JavaPreparersBuilder struct {
	PreparersBuilder
}

//JavaPreparers chains to type *PreparersBuilder and returns a *JavaPreparersBuilder
func (builder *PreparersBuilder) JavaPreparers() *JavaPreparersBuilder {
	return &JavaPreparersBuilder{*builder}
}

//WithPublicClassRemover adds preparer to remove public class
func (builder *JavaPreparersBuilder) WithPublicClassRemover() *JavaPreparersBuilder {
	removePublicClassPreparer := Preparer{
		Prepare: removePublicClassModifier,
		Args:    []interface{}{builder.filePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern},
	}
	builder.AddPreparer(removePublicClassPreparer)
	return builder
}

//WithPackageChanger adds preparer to change package
func (builder *JavaPreparersBuilder) WithPackageChanger() *JavaPreparersBuilder {
	changePackagePreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, packagePattern, importStringPattern},
	}
	builder.AddPreparer(changePackagePreparer)
	return builder
}

//WithPackageRemover adds preparer to remove package
func (builder *JavaPreparersBuilder) WithPackageRemover() *JavaPreparersBuilder {
	removePackagePreparer := Preparer{
		Prepare: replace,
		Args:    []interface{}{builder.filePath, packagePattern, newLinePattern},
	}
	builder.AddPreparer(removePackagePreparer)
	return builder
}

//WithFileNameChanger adds preparer to remove package
func (builder *JavaPreparersBuilder) WithFileNameChanger() *JavaPreparersBuilder {
	unitTestFileNameChanger := Preparer{
		Prepare: changeJavaTestFileName,
		Args:    []interface{}{builder.filePath},
	}
	builder.AddPreparer(unitTestFileNameChanger)
	return builder
}

//WithGraphHandler adds code to save the graph
func (builder *JavaPreparersBuilder) WithGraphHandler() *JavaPreparersBuilder {
	graphCodeAdder := Preparer{
		Prepare: addCodeToSaveGraph,
		Args:    []interface{}{builder.filePath},
	}
	builder.AddPreparer(graphCodeAdder)
	return builder
}

func addCodeToSaveGraph(args ...interface{}) error {
	filePath := args[0].(string)
	pipelineObjectName, _ := findPipelineObjectName(filePath)
	graphSaveCode := fmt.Sprintf(graphSavePattern, pipelineObjectName, utils.GraphFileName)

	if pipelineObjectName != utils.EmptyLine {
		reg := regexp.MustCompile(fmt.Sprintf(graphRunPattern, pipelineObjectName))
		code, err := ioutil.ReadFile(filePath)
		if err != nil {
			logger.Error("Can't read file")
			return err
		}
		result := reg.ReplaceAllString(string(code), fmt.Sprintf(`%s$1`, graphSaveCode))
		if err != nil {
			logger.Error("Can't add graph extraction code")
			return err
		}
		if err = ioutil.WriteFile(filePath, []byte(result), 0666); err != nil {
			logger.Error("Can't rewrite file %s", filePath)
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
			WithGraphHandler()
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
			WithGraphHandler()
	}
}

// findPipelineObjectName finds name of pipeline in JAVA code when pipeline is created
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

	tmp, err := createTempFile(filePath)
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

// createTempFile creates temporary file next to originalFile
func createTempFile(originalFilePath string) (*os.File, error) {
	// all folders which are included in filePath
	filePathSlice := strings.Split(originalFilePath, string(pathSeparatorPattern))
	fileName := filePathSlice[len(filePathSlice)-1]

	tmpFileName := fmt.Sprintf("%s_%s", tmpFileSuffix, fileName)
	tmpFilePath := strings.Replace(originalFilePath, fileName, tmpFileName, 1)
	return os.Create(tmpFilePath)
}

func changeJavaTestFileName(args ...interface{}) error {
	filePath := args[0].(string)
	className, err := utils.GetPublicClassName(filePath, javaPublicClassNamePattern)
	if err != nil {
		return err
	}
	err = utils.RenameSourceCodeFile(filePath, className)
	if err != nil {
		return err
	}
	return nil
}
