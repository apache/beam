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

package jobopts

import (
	"fmt"
)

// stringSlice is a flag.Value implementation for string slices, that allows
// multiple strings to be assigned to one flag by specifying multiple instances
// of the flag.
//
// Example:
//
//	var myFlags stringSlice
//	flag.Var(&myFlags, "my_flag", "A list of flags")
//	$cmd -my_flag foo -my_flag bar
//
// With the example above, the slice can be set to contain ["foo", "bar"]:
type stringSlice []string

// String implements the String method of flag.Value. This outputs the value
// of the flag as a string.
func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

// Set implements the Set method of flag.Value. This stores a string input to
// the flag into a stringSlice representation.
func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Get returns the instance itself.
func (s stringSlice) Get() any {
	return s
}
