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

package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"testing"

	yaml "gopkg.in/yaml.v2"
)

func TestStandardCoders(t *testing.T) {
	data, err := ioutil.ReadFile(yamlPath)
	if err != nil {
		log.Fatalf("Couldn't read %v: %v", yamlPath, err)
	}
	specs := bytes.Split(data, []byte("\n---\n"))
	var l logLogger
	for _, data := range specs {
		cs := Spec{Log: t}
		if err := yaml.Unmarshal(data, &cs); err != nil {
			l.Logf("unable to parse yaml: %v %q", err, data)
			continue
		}
		t.Run(cs.Coder.Urn, func(t *testing.T) {
			if err := cs.testStandardCoder(); err != nil {
				t.Errorf("Failed \"%v\": %v", cs.Coder, err)
			}
		})
	}

}
