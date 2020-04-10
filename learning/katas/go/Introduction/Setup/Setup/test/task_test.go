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

package test

import (
	"io/ioutil"
	"task"
	"testing"
)

const (
	outputPath = "output.txt"
)

func TestTask(t *testing.T) {
	err := task.Task()
	if err != nil {
		t.Error(err)
	}
	data, err := ioutil.ReadFile(outputPath)
	if err != nil {
		t.Error(err)
	}

	want := "This proves your pipeline is running.\n"
	got := string(data)
	if want != got {
		t.Errorf("want: %s got: %s", want, got)
	}

	// This is intended to clear the output.txt to revert back to the original state
	err = ioutil.WriteFile(outputPath, []byte{}, 0644)
	if err != nil {
		t.Fatal(err)
	}
}
