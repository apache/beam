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

//beam:input beam:coder:varint:v1
//beam:output beam:coder:varint:v1

//go:generate tinygo build -o add.wasm -target=wasi add.go
package main

import (
	"encoding/binary"
	"github.com/extism/go-pdk"
)

/*
	Usage:
		plugin, err := extism.NewPlugin(...)
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, 2)
		_, out, err := plugin.Call("ProcessElement", data)
		got := binary.LittleEndian.Uint64(out)
		fmt.Print(got) // 4
*/
//export ProcessElement
func ProcessElement() uint32 {
	out := make([]byte, 8)
	data := pdk.Input()
	element := binary.LittleEndian.Uint64(data)
	binary.LittleEndian.PutUint64(out, element+element)
	pdk.Output(out)

	return 0
}

// main is required for the `wasi` target, even if it isn't used.
// See https://wazero.io/languages/tinygo/#why-do-i-have-to-define-main
func main() {}
