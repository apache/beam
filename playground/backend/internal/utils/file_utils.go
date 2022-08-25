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
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
	"path/filepath"
	"strings"
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
	goExt                 = ".go"
	pythonExt             = ".py"
	scioExt               = ".scala"
)

// GetFileName returns the valid file name.
func GetFileName(name string, sdk pb.Sdk) string {
	if name == "" {
		logger.Warn("The name of the file is empty. Will be used default value")
		switch sdk {
		case pb.Sdk_SDK_JAVA:
			return defaultJavaFileName
		case pb.Sdk_SDK_GO:
			return defaultGoFileName
		case pb.Sdk_SDK_PYTHON:
			return defaultPythonFileName
		case pb.Sdk_SDK_SCIO:
			return defaultScioFileName
		}
	}
	return getCorrectFileName(name, sdk)
}

// getCorrectFileName returns the correct file name.
func getCorrectFileName(name string, sdk pb.Sdk) string {
	ext := filepath.Ext(name)
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return getCorrectNameOrDefault(ext, javaExt, defaultJavaFileName, name)
	case pb.Sdk_SDK_GO:
		return getCorrectNameOrDefault(ext, goExt, defaultGoFileName, name)
	case pb.Sdk_SDK_PYTHON:
		return getCorrectNameOrDefault(ext, pythonExt, defaultPythonFileName, name)
	case pb.Sdk_SDK_SCIO:
		return getCorrectNameOrDefault(ext, scioExt, defaultScioFileName, name)
	default:
		return name
	}
}

// getCorrectNameOrDefault returns the correct file name or default name.
func getCorrectNameOrDefault(actualExt, correctExt, defaultFileName, name string) string {
	if actualExt == "" {
		logger.Error("The name of the file does not have extension. Will be used default value")
		return defaultFileName
	}
	if actualExt != correctExt {
		logger.Error("The name of the file has wrong extension. Will be used correct extension according to sdk")
		return name[0:len(name)-len(actualExt)] + correctExt
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
