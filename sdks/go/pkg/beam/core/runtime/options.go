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

package runtime

import (
	"sync"
)

// GlobalOptions are the global options for the active graph. Options can be
// defined at any time before execution and are re-created by the harness on
// remote execution workers. Global options should be used sparingly.
var GlobalOptions = &Options{opt: make(map[string]string)}

// Options are untyped options.
type Options struct {
	opt map[string]string
	ro  bool
	mu  sync.Mutex
}

// RawOptions represents exported options as JSON-serializable data.
// Exact representation is subject to change.
type RawOptions struct {
	Options map[string]string `json:"options"`
}

// TODO(herohde) 7/7/2017: Dataflow has a concept of sdk pipeline options and
// various values in this map:
//
// { "display_data": [...],
//   "options":{
//      "autoscalingAlgorithm":"NONE",
//      "dataflowJobId":"2017-07-07_xxx",
//      "gcpTempLocation":"",
//      "maxNumWorkers":0,
//      "numWorkers":1,
//      "project":"xxx",
//      "options": <Go SDK pipeline options>,
//  }}
//
// Which we may or may not want to be visible as first-class pipeline options.
// It is also TBD how/if to support global display data, but we certainly don't
// want it served back to the harness.

// TODO(herohde) 3/12/2018: remove the extra options wrapper and the bogus
// fields current required by the Java runners.

// RawOptionsWrapper wraps RawOptions to the form expected by the
// harness. The extra layer is currently needed due to Dataflow
// expectations about this representation. Subject to change.
type RawOptionsWrapper struct {
	Options      RawOptions `json:"beam:option:go_options:v1"`
	Runner       string     `json:"beam:option:runner:v1"`
	AppName      string     `json:"beam:option:app_name:v1"`
	Experiments  []string   `json:"beam:option:experiments:v1"`
	RetainDocker bool       `json:"beam:option:retain_docker_containers:v1"`
}

// Import imports the options from previously exported data and makes the
// options read-only. It panics if import is called twice.
func (o *Options) Import(opt RawOptions) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ro {
		panic("import failed: options read-only")
	}
	o.ro = true
	o.opt = copyMap(opt.Options)
}

// Get returns the value of the key. If the key has not been set, it returns "".
func (o *Options) Get(key string) string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.opt[key]
}

// Set defines the value of the given key. If the key is already defined, it
// panics.
func (o *Options) Set(key, value string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.ro {
		return // ignore silently to allow init-time set of options
	}
	o.opt[key] = value
}

// Export returns a JSON-serializable copy of the options.
func (o *Options) Export() RawOptions {
	o.mu.Lock()
	defer o.mu.Unlock()

	return RawOptions{Options: copyMap(o.opt)}
}

func copyMap(m map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range m {
		ret[k] = v
	}
	return ret
}
