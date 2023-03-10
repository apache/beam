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
	"beam.apache.org/playground/backend/internal/emulators"
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"

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
	graphSavePattern                  = "String dotString = org.apache.beam.runners.core.construction.renderer.PipelineDotRenderer.toDotString(%s);\n" +
		"    try (java.io.PrintWriter out = new java.io.PrintWriter(\"graph.dot\")) {\n      " +
		"		out.println(dotString);\n    " +
		"	} catch (java.io.FileNotFoundException e) {\n" +
		"      e.printStackTrace();\n    " +
		"\n}\n" +
		"%s.run"
)

type javaPreparer struct {
	preparer
	isKata     bool
	isUnitTest bool
	parameters *emulators.EmulatorParameters
}

func GetJavaPreparer(filePath string, isUnitTest bool, isKata bool, parameters *emulators.EmulatorParameters) Preparer {
	return javaPreparer{
		preparer: preparer{
			filePath: filePath,
		},
		isKata:     isKata,
		isUnitTest: isUnitTest,
		parameters: parameters,
	}
}

func (p javaPreparer) Prepare() error {
	if !p.isUnitTest && !p.isKata {
		if err := removePublicClassModifier(p.filePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern); err != nil {
			return err
		}

		// Replace 'package' with 'import'
		if err := replace(p.filePath, packagePattern, importStringPattern); err != nil {
			return err
		}

		if err := addCodeToSaveGraph(p.filePath); err != nil {
			return err
		}

		if p.parameters != nil {
			// Replace bootstrap server address placeholder
			if err := replace(p.filePath, bootstrapServerPattern, p.parameters.BootstrapServer); err != nil {
				return err
			}
			// Replace topic name
			if err := replace(p.filePath, topicNamePattern, p.parameters.TopicName); err != nil {
				return err
			}
		}
	}
	if p.isUnitTest {
		// Replace 'package' with 'import'
		if err := replace(p.filePath, packagePattern, importStringPattern); err != nil {
			return err
		}
		// Rename test file to match class name
		if err := utils.ChangeTestFileName(p.filePath, javaPublicClassNamePattern); err != nil {
			return err
		}
	}
	if p.isKata {
		if err := removePublicClassModifier(p.filePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern); err != nil {
			return err
		}

		// Remove 'package' line
		if err := replace(p.filePath, packagePattern, newLinePattern); err != nil {
			return err
		}

		if err := addCodeToSaveGraph(p.filePath); err != nil {
			return err
		}

		if p.parameters != nil {
			// Replace bootstrap server address placeholder
			if err := replace(p.filePath, bootstrapServerPattern, p.parameters.BootstrapServer); err != nil {
				return err
			}
			// Replace topic name
			if err := replace(p.filePath, topicNamePattern, p.parameters.TopicName); err != nil {
				return err
			}
		}
	}
	return nil
}

func addCodeToSaveGraph(args ...interface{}) error {
	filePath := args[0].(string)
	pipelineObjectName, _ := findPipelineObjectName(filePath)
	graphSaveCode := fmt.Sprintf(graphSavePattern, pipelineObjectName, pipelineObjectName)

	if pipelineObjectName != utils.EmptyLine {
		err := replace(filePath, fmt.Sprintf("%s.run", pipelineObjectName), graphSaveCode)
		if err != nil {
			logger.Error("Can't add graph extractor. Can't add new import")
			return err
		}
	}
	return nil
}

// findPipelineObjectName finds name of pipeline in JAVA code when pipeline creates
func findPipelineObjectName(filepath string) (string, error) {
	reg := regexp.MustCompile(pipelineNamePattern)
	b, err := os.ReadFile(filepath)
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
func replace(filePath, pattern, newPattern string) error {
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

func removePublicClassModifier(filePath, pattern, newPattern string) error {
	err := replace(filePath, pattern, newPattern)
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
