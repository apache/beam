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
	"path/filepath"
	"strings"

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
)

const (
	javaMainMethod        = "public static void main(String[] args)"
	goMainMethod          = "func main()"
	pythonMainMethod      = "if __name__ == '__main__'"
	scioMainMethod        = "def main(cmdlineArgs: Array[String])"
	defaultJavaFileName   = "main.java"
	defaultGoFileName     = "main.go"
	defaultPythonFileName = "main.py"
	defaultScioFileName   = "main.scala"
	javaExt               = ".java"
	classExt              = ".class"
	goExt                 = ".go"
	pythonExt             = ".py"
	scioExt               = ".scala"

	mismatchExtAndSDKErrMsg = "file extension doesn't match to the specified SDK"
)

// GetFileName returns the valid file name.
func GetFileName(name, content string, sdk pb.Sdk) (string, error) {
	if name == "" {
		logger.Warn("The name of the file is empty. Will be used default value")
		switch sdk {
		case pb.Sdk_SDK_JAVA:
			return defaultJavaFileName, nil
		case pb.Sdk_SDK_GO:
			return defaultGoFileName, nil
		case pb.Sdk_SDK_PYTHON:
			return defaultPythonFileName, nil
		case pb.Sdk_SDK_SCIO:
			return defaultScioFileName, nil
		}
	}
	return getCorrectFileName(name, content, sdk)
}

// getCorrectFileName returns the correct file name.
func getCorrectFileName(name, content string, sdk pb.Sdk) (string, error) {
	ext := getExtOrExtBasedOnContent(filepath.Ext(name), content)
	if ext != "" && !isValidFileExtensionAndSDK(ext, sdk) {
		return "", errors.New(mismatchExtAndSDKErrMsg)
	}
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return getCorrectNameOrDefault(ext, javaExt, defaultJavaFileName, name), nil
	case pb.Sdk_SDK_GO:
		return getCorrectNameOrDefault(ext, goExt, defaultGoFileName, name), nil
	case pb.Sdk_SDK_PYTHON:
		return getCorrectNameOrDefault(ext, pythonExt, defaultPythonFileName, name), nil
	case pb.Sdk_SDK_SCIO:
		return getCorrectNameOrDefault(ext, scioExt, defaultScioFileName, name), nil
	default:
		return name, nil
	}
}

// isValidFileExtensionAndSDK returns a flag indicating the result of validation
func isValidFileExtensionAndSDK(ext string, sdk pb.Sdk) bool {
	switch ext {
	case javaExt:
		return sdk == pb.Sdk_SDK_JAVA
	case goExt:
		return sdk == pb.Sdk_SDK_GO
	case pythonExt:
		return sdk == pb.Sdk_SDK_PYTHON
	case scioExt:
		return sdk == pb.Sdk_SDK_SCIO
	default:
		return false
	}
}

// getExtBasedOnContent return a file extension
func getExtOrExtBasedOnContent(ext, content string) string {
	if ext == "" {
		return getExtBasedOnContent(content)
	}
	return ext
}

// getExtBasedOnContent return a file extension based on the content
func getExtBasedOnContent(content string) string {
	if strings.Contains(content, javaMainMethod) {
		return javaExt
	}
	if strings.Contains(content, goMainMethod) {
		return goExt
	}
	if strings.Contains(content, pythonMainMethod) {
		return pythonExt
	}
	if strings.Contains(content, scioMainMethod) {
		return scioExt
	}
	return ""
}

// getCorrectNameOrDefault returns the correct file name or default name.
func getCorrectNameOrDefault(actualExt, correctExt, defaultFileName, name string) string {
	if actualExt == "" {
		logger.Infof("The name of the file does not have extension. Default value (%s) will be used", correctExt)
		if name == "" {
			return defaultFileName
		}
		return name + correctExt
	}
	if actualExt != correctExt {
		logger.Infof("The name of the file has wrong extension. Correct extension (%s) will be used according to sdk", correctExt)
		return name[0:len(name)-len(actualExt)] + correctExt
	}
	if filepath.Ext(name) == "" {
		return name + correctExt
	}
	return name
}

// IsFileMain returns true if the file content has a main function, otherwise false.
func IsFileMain(content string, sdk pb.Sdk) bool {
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return strings.Contains(content, javaMainMethod)
	case pb.Sdk_SDK_GO:
		return strings.Contains(content, goMainMethod)
	case pb.Sdk_SDK_PYTHON:
		return strings.Contains(content, pythonMainMethod)
	case pb.Sdk_SDK_SCIO:
		return strings.Contains(content, scioMainMethod)
	default:
		return false
	}
}

// ToSDKFromExt returns SDK according to a specified extension.
func ToSDKFromExt(ext string) pb.Sdk {
	switch ext {
	case javaExt, classExt:
		return pb.Sdk_SDK_JAVA
	case goExt:
		return pb.Sdk_SDK_GO
	case scioExt:
		return pb.Sdk_SDK_SCIO
	case pythonExt:
		return pb.Sdk_SDK_PYTHON
	default:
		return pb.Sdk_SDK_UNSPECIFIED
	}
}

func TrimExtension(filename string) string {
	return strings.TrimSuffix(filename, filepath.Ext(filename))
}
