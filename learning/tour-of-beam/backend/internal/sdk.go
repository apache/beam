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
	SDK_GO        Sdk = "Go"
	SDK_PYTHON    Sdk = "Python"
	SDK_JAVA      Sdk = "Java"
	SDK_SCIO      Sdk = "SCIO"
)

func (s Sdk) String() string {
	return string(s)
}

// Parse sdk from string names, f.e. "Java" -> Sdk.GO_JAVA
// Returns SDK_UNDEFINED on error.
func ParseSdk(s string) Sdk {
	switch s {
	case "Go":
		return SDK_GO
	case "Python":
		return SDK_PYTHON
	case "Java":
		return SDK_JAVA
	case "SCIO":
		return SDK_SCIO
	default:
		return SDK_UNDEFINED
	}
}

func SdksList() [4]string {
	return [4]string{"Java", "Python", "Go", "SCIO"}
}
