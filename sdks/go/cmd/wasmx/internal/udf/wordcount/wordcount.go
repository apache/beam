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

//beam:input beam:coder:string_utf8:v1
//beam:output beam:coder:varint:v1

//go:generate tinygo build -o wordcount.wasm -target=wasi wordcount.go
package main

import (
	"encoding/binary"
	"github.com/extism/go-pdk"
	"strings"
)

// main is required for TinyGo to compile to Wasm.
func main() {}

/*
	Usage:
		plugin, err := extism.NewPlugin(...)
		_, out, err := plugin.Call("ProcessElement", []byte("some string input"))
		got := binary.LittleEndian.Uint64(out)
		fmt.Print(got) // output: 3
*/
//export ProcessElement
func ProcessElement() int32 {
	var tokens []string
	b := make([]byte, 8)
	input := pdk.InputString()
	input = strings.TrimSpace(input)
	for _, tok := range strings.Split(input, " ") {
		tok = strings.TrimSpace(tok)
		if tok != "" {
			tokens = append(tokens, tok)
		}
	}
	binary.LittleEndian.PutUint64(b, uint64(len(tokens)))
	pdk.Output(b)
	return 0
}
