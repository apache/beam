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
	"beam.apache.org/playground/backend/internal/logger"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"regexp"
)

func ReduceWhiteSpacesToSinge(s string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(s, " ")
}

//ReadFile reads from file and returns string.
func ReadFile(pipelineId uuid.UUID, path string) (string, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		logger.Errorf("%s: ReadFile(): error during reading from a file: %s", pipelineId, err.Error())
		return "", err
	}
	return string(content), nil
}

//ReadYamlFile reads from a yaml file.
func ReadYamlFile(filename string, out interface{}) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Errorf("ReadYamlFile(): error during reading from a yaml file: %s", err.Error())
		return err
	}
	if err = yaml.Unmarshal(buf, out); err != nil {
		logger.Errorf("ReadYamlFile(): error during parsing from a yaml file: %s", err.Error())
		return err
	}
	return nil
}
