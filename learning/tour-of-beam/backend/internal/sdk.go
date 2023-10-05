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

package internal

type Sdk string

const (
	SDK_UNDEFINED Sdk = ""
	SDK_GO        Sdk = "go"
	SDK_PYTHON    Sdk = "python"
	SDK_JAVA      Sdk = "java"
	SDK_SCIO      Sdk = "scio"
)

func (s Sdk) String() string {
	return string(s)
}

// get Title which is shown on the landing page
func (s Sdk) Title() string {
	switch s {
	case SDK_GO:
		return "Go"
	case SDK_JAVA:
		return "Java"
	case SDK_PYTHON:
		return "Python"
	case SDK_SCIO:
		return "SCIO"
	default:
		panic("undefined/unknown SDK title")
	}
}

// SDK type representation in datastore
func (s Sdk) StorageID() string {
	switch s {
	case SDK_GO:
		return "SDK_GO"
	case SDK_PYTHON:
		return "SDK_PYTHON"
	case SDK_JAVA:
		return "SDK_JAVA"
	case SDK_SCIO:
		return "SDK_SCIO"
	}
	panic("undefined storage id for sdk")
}

// Parse sdk from string names, f.e. "java" -> Sdk.GO_JAVA
// Make allowance for the case if the Title is given, not Id
// Returns SDK_UNDEFINED on error.
func ParseSdk(s string) Sdk {
	switch s {
	case "go", "Go":
		return SDK_GO
	case "python", "Python":
		return SDK_PYTHON
	case "java", "Java":
		return SDK_JAVA
	case "scio", "SCIO":
		return SDK_SCIO
	default:
		return SDK_UNDEFINED
	}
}

func MakeSdkList() SdkList {
	sdks := make([]SdkItem, 0, 3)
	for _, sdk := range []Sdk{SDK_JAVA, SDK_PYTHON, SDK_GO} {
		sdks = append(sdks, SdkItem{Id: sdk.String(), Title: sdk.Title()})
	}
	return SdkList{Sdks: sdks}
}
