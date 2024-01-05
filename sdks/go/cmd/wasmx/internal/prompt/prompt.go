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

// Package prompt supports acquiring user confirmation and information.
package prompt

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// YN prompts user with yes or no prompt.
// Non-nil def argument defaults the prompt.
func YN(message string, def *bool) error {
	p := &Prompt[bool]{
		Message: message,
		Values: map[string]bool{
			"y": true,
			"n": false,
		},
		Alias: map[string]string{
			"yes": "y",
			"no":  "n",
		},
		def:     false,
		display: "",
	}
	return p.Run(def)
}

type Result interface {
	bool | string
}

type Prompt[R Result] struct {
	Message string
	Values  map[string]R
	Alias   map[string]string
	def     R // default value
	display string
}

func (p *Prompt[R]) show() string {
	if p.display != "" {
		return p.display
	}

	var allowed []string
	for k, v := range p.Values {
		if v == p.def {
			k = strings.ToUpper(k)
		}
		allowed = append(allowed, k)
	}
	sort.Strings(allowed)

	p.display = fmt.Sprintf("%s (%s) ", p.Message, strings.Join(allowed, "/"))

	return p.display
}

// Run the prompt until user provides result.
// Non-nil result argument assigns def value.
// Returns error when non-nil result or def does not map to Prompt Values.
func (p *Prompt[R]) Run(result *R) error {
	if result == nil {
		return p.run(os.Stdin, result)
	}
	if !p.hasKey(result) {
		return fmt.Errorf("error: result argument does not map to a known key: %v", p.def)
	}
	p.def = *result

	return p.run(os.Stdin, result)
}

func (p *Prompt[R]) hasKey(value *R) bool {
	for _, v := range p.Values {
		if v == *value {
			return true
		}
	}
	return false
}

func (p *Prompt[R]) run(r io.Reader, result *R) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if scanner.Err() != nil {
			return scanner.Err()
		}
		text := scanner.Text()
		text = strings.ToLower(text)
		text = strings.TrimSpace(text)
		if alias, ok := p.Alias[text]; ok {
			text = alias
		}
		if v, ok := p.Values[text]; ok {
			*result = v
			return nil
		}
	}
	result = &p.def
	return nil
}
