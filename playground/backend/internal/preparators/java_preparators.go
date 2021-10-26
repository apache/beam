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

package preparators

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

const (
	classWithPublicModifierPattern    = "public class "
	classWithoutPublicModifierPattern = "class "
	packagePattern                    = `package ([\w]+\.)+[\w]+;`
	emptyStringPattern                = ""
	newLinePattern                    = "\n"
	pathSeparatorPattern              = os.PathSeparator
	tmpFileSuffix                     = "tmp"
)

// GetJavaPreparation returns preparation methods that should be applied to Java code
func GetJavaPreparation(filePath string) *[]Preparator {
	publicClassModification := Preparator{
		Prepare: replace,
		Args:    []interface{}{filePath, classWithPublicModifierPattern, classWithoutPublicModifierPattern},
	}
	additionalPackage := Preparator{
		Prepare: replace,
		Args:    []interface{}{filePath, packagePattern, emptyStringPattern},
	}
	preparation := []Preparator{publicClassModification, additionalPackage}
	return &preparation
}

// replace processes file by filePath and replaces all patterns to newPattern
func replace(args ...interface{}) error {
	filePath := args[0].(string)
	pattern := args[1].(string)
	newPattern := args[2].(string)

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Preparation: Error during open file: %s, err: %s\n", filePath, err.Error())
		return err
	}
	defer file.Close()

	tmp, err := createTempFile(filePath)
	if err != nil {
		log.Printf("Preparation: Error during create new temporary file, err: %s\n", err.Error())
		return err
	}
	defer tmp.Close()

	// uses to indicate when need to add new line to tmp file
	err = writeWithReplace(file, tmp, pattern, newPattern)
	if err != nil {
		log.Printf("Preparation: Error during write data to tmp file, err: %s\n", err.Error())
		return err
	}

	// replace original file with temporary file with renaming
	if err = os.Rename(tmp.Name(), filePath); err != nil {
		log.Printf("Preparation: Error during rename temporary file, err: %s\n", err.Error())
		return err
	}
	return nil
}

// writeWithReplace writes data with replacing all patterns to newPattern
func writeWithReplace(from *os.File, to *os.File, pattern, newPattern string) error {
	newLine := false
	reg := regexp.MustCompile(pattern)
	scanner := bufio.NewScanner(from)

	for scanner.Scan() {
		line := scanner.Text()
		err := replaceAndWriteLine(newLine, to, line, reg, newPattern)
		if err != nil {
			log.Printf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
			return err
		}
		newLine = true
	}
	return scanner.Err()
}

// replaceAndWriteLine replaces pattern to newPattern and writes line to file
func replaceAndWriteLine(newLine bool, to *os.File, line string, reg *regexp.Regexp, newPattern string) error {
	err := addNewLine(newLine, to)
	if err != nil {
		log.Printf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", newLinePattern, err.Error())
		return err
	}
	matches := reg.FindAllString(line, -1)
	for _, str := range matches {
		line = strings.ReplaceAll(line, str, newPattern)
	}
	if _, err = io.WriteString(to, line); err != nil {
		log.Printf("Preparation: Error during write \"%s\" to tmp file, err: %s\n", line, err.Error())
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

// addNewLine adds a new line at the end of the file
func addNewLine(newLine bool, file *os.File) error {
	if !newLine {
		return nil
	}
	if _, err := io.WriteString(file, newLinePattern); err != nil {
		return err
	}
	return nil
}
