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

// Package environment provides helpers for interacting with environment variables.
package environment

import (
	"fmt"
	"os"
	"strings"
)

// Variable defines an environment variable via a string type alias.
// Variable's string defaultValue assigns the system environment variable key.
type Variable string

// Default a defaultValue to the system environment.
func (v Variable) Default(value string) error {
	if v.Missing() {
		return os.Setenv((string)(v), value)
	}
	return nil
}

// Missing reports whether the system environment variable is an empty string.
func (v Variable) Missing() bool {
	return v.Value() == ""
}

// Key returns the system environment variable key.
func (v Variable) Key() string {
	return (string)(v)
}

// Value returns the system environment variable defaultValue.
func (v Variable) Value() string {
	return os.Getenv((string)(v))
}

// KeyValue returns a concatenated string of the system environment variable's
// <key>=<defaultValue>.
func (v Variable) KeyValue() string {
	return fmt.Sprintf("%s=%s", (string)(v), v.Value())
}

// Missing reports as an error listing all Variable among vars that are
// not assigned in the system environment.
func Missing(vars ...Variable) error {
	var missing []string
	for _, v := range vars {
		if v.Missing() {
			missing = append(missing, v.KeyValue())
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("variables empty but expected from environment: %s", strings.Join(missing, "; "))
	}
	return nil
}

// Map converts a slice of Variable into a map.
// Its usage is for logging purposes.
func Map(vars ...Variable) map[string]interface{} {
	result := map[string]interface{}{}
	for _, v := range vars {
		result[(string)(v)] = v.Value()
	}
	return result
}
