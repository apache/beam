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

package fs_content

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v3"
)

// Could have done it in generics if 1.18 was supported in GCF
// Fatals on error.
func loadLearningPathInfo(path string) (info learningPathInfo) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(buf, &info)
	if err != nil {
		log.Fatal(err)
	}

	return info
}

func loadLearningModuleInfo(path string) (info learningModuleInfo) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(buf, &info)
	if err != nil {
		log.Fatal(err)
	}

	return info
}

func loadLearningGroupInfo(path string) (info learningGroupInfo) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(buf, &info)
	if err != nil {
		log.Fatal(err)
	}

	return info
}

func loadLearningUnitInfo(path string) (info learningUnitInfo) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(buf, &info)
	if err != nil {
		log.Fatal(err)
	}

	return info
}
